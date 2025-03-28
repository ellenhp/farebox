use std::{
    fs::{self, File},
    marker::PhantomData,
    mem::size_of,
    path::PathBuf,
    pin::Pin,
    slice,
};

use anyhow::{Error, Ok};
use bytemuck::{cast_slice_mut, checked::cast_slice};
use log::{debug, info, warn};
use memmap2::{Mmap, MmapMut, MmapOptions};
use redb::Database;
use reqwest::Client;
use rstar::RTree;
use s2::latlng::LatLng;

use crate::{
    raptor::geomath::{
        lat_lng_to_cartesian, IndexedStop, EARTH_RADIUS_APPROX, FAKE_WALK_SPEED_SECONDS_PER_METER,
    },
    valhalla::{matrix_request, MatrixRequest, ValhallaLocation},
};

use super::{
    in_memory::InMemoryTimetableBuilder, Route, RouteStop, ShapeCoordinate, Stop, StopRoute,
    Timetable, Transfer, Trip, TripMetadata, TripStopTime, ROUTE_SHAPE_TABLE, STOP_METADATA_TABLE,
    TRIP_METADATA_TABLE,
};

#[allow(unused)]
pub struct MmapTimetable<'a> {
    base_path: PathBuf,

    backing_routes: Pin<Mmap>,
    backing_route_stops: Pin<Mmap>,
    backing_route_trips: Pin<Mmap>,
    backing_stops: Pin<Mmap>,
    backing_stop_routes: Pin<Mmap>,
    backing_trip_stop_times: Pin<Mmap>,
    backing_transfer_index: Pin<Mmap>,
    backing_transfers: Pin<Mmap>,

    routes_slice: &'a [Route],
    route_stops_slice: &'a [RouteStop],
    route_trips_slice: &'a [Trip],
    stops_slice: &'a [Stop],
    stop_routes_slice: &'a [StopRoute],
    trip_stop_times_slice: &'a [TripStopTime],
    transfer_index_slice: &'a [usize],
    transfers_slice: &'a [Transfer],
    rtree: RTree<IndexedStop>,

    metadata_db: redb::Database,

    phantom: &'a PhantomData<()>,

    // reqwest client.
    client: Client,
}

impl<'a> Timetable<'a> for MmapTimetable<'a> {
    #[inline]
    fn route(&'a self, route_id: usize) -> &'a Route {
        &self.routes()[route_id as usize]
    }

    #[inline]
    fn stop(&'a self, stop_id: usize) -> &'a Stop {
        &self.stops()[stop_id as usize]
    }

    #[inline]
    fn transfers_from(&'a self, stop_id: usize) -> &'a [Transfer] {
        Transfer::all_transfers(self.stop(stop_id), self)
    }

    #[inline]
    fn stop_count(&self) -> usize {
        self.stops().len()
    }

    #[inline]
    fn stops(&'a self) -> &'a [Stop] {
        self.stops_slice
    }

    #[inline]
    fn stop_routes(&'a self) -> &'a [StopRoute] {
        self.stop_routes_slice
    }

    #[inline]
    fn routes(&'a self) -> &'a [Route] {
        self.routes_slice
    }

    #[inline]
    fn route_stops(&'a self) -> &'a [RouteStop] {
        self.route_stops_slice
    }

    #[inline]
    fn route_trips(&'a self) -> &'a [Trip] {
        self.route_trips_slice
    }

    #[inline]
    fn trip_stop_times(&'a self) -> &'a [TripStopTime] {
        self.trip_stop_times_slice
    }

    #[inline]
    fn transfers(&'a self) -> &'a [Transfer] {
        self.transfers_slice
    }

    #[inline]
    fn transfer_index(&'a self) -> &'a [usize] {
        self.transfer_index_slice
    }

    #[inline]
    fn stop_index_copy(&'a self) -> RTree<IndexedStop> {
        self.rtree.clone()
    }

    fn nearest_stops(&'a self, lat: f64, lng: f64, n: usize) -> Vec<(&'a Stop, f64)> {
        self.rtree
            .nearest_neighbor_iter_with_distance_2(&lat_lng_to_cartesian(lat, lng))
            .take(n)
            .map(|(stop, dist_sq)| (self.stop(stop.id), dist_sq.sqrt()))
            .collect()
    }

    fn stop_metadata(&'a self, stop: &Stop) -> gtfs_structures::Stop {
        let table = self
            .metadata_db
            .begin_read()
            .expect("Read failed")
            .open_table(STOP_METADATA_TABLE)
            .expect("Failed to open table");

        let bytes = table
            .get(stop.id() as u64)
            .expect("DB error")
            .expect("Missing metadata for stop");
        rmp_serde::from_slice(bytes.value()).expect("Deserialization failed")
    }

    fn trip_metadata(&'a self, trip: &Trip) -> TripMetadata {
        let table = self
            .metadata_db
            .begin_read()
            .expect("Read failed")
            .open_table(TRIP_METADATA_TABLE)
            .expect("Failed to open table");

        let bytes = table
            .get(trip.trip_index as u64)
            .expect("DB error")
            .expect("Missing metadata for trip");
        rmp_serde::from_slice(bytes.value()).expect("Deserialization failed")
    }

    fn route_shape(&'a self, route: &Route) -> Option<Vec<ShapeCoordinate>> {
        let table = self
            .metadata_db
            .begin_read()
            .expect("Read failed")
            .open_table(ROUTE_SHAPE_TABLE)
            .expect("Failed to open table");

        if let Some(bytes) = table.get(route.route_index as u64).expect("DB error") {
            rmp_serde::from_slice(bytes.value()).expect("Deserialization failed")
        } else {
            None
        }
    }
}

impl<'a> MmapTimetable<'a> {
    fn assemble(
        base_path: PathBuf,
        backing_routes: Pin<Mmap>,
        backing_route_stops: Pin<Mmap>,
        backing_route_trips: Pin<Mmap>,
        backing_stops: Pin<Mmap>,
        backing_stop_routes: Pin<Mmap>,
        backing_trip_stop_times: Pin<Mmap>,
        backing_transfer_index: Pin<Mmap>,
        backing_transfers: Pin<Mmap>,
        metadata_db: Database,
    ) -> Result<MmapTimetable<'a>, anyhow::Error> {
        let routes = unsafe {
            let s = cast_slice::<u8, Route>(&backing_routes);
            slice::from_raw_parts(s.as_ptr(), s.len())
        };
        let route_stops = unsafe {
            let s = cast_slice::<u8, RouteStop>(&backing_route_stops);
            slice::from_raw_parts(s.as_ptr(), s.len())
        };
        let route_trips = unsafe {
            let s = cast_slice::<u8, Trip>(&backing_route_trips);
            slice::from_raw_parts(s.as_ptr(), s.len())
        };
        let stops = unsafe {
            let s = cast_slice::<u8, Stop>(&backing_stops);
            slice::from_raw_parts(s.as_ptr(), s.len())
        };
        let stop_routes = unsafe {
            let s = cast_slice::<u8, StopRoute>(&backing_stop_routes);
            slice::from_raw_parts(s.as_ptr(), s.len())
        };
        let trip_stop_times = unsafe {
            let s = cast_slice::<u8, TripStopTime>(&backing_trip_stop_times);
            slice::from_raw_parts(s.as_ptr(), s.len())
        };
        let transfer_index = unsafe {
            let s = cast_slice::<u8, usize>(&backing_transfer_index);
            slice::from_raw_parts(s.as_ptr(), s.len())
        };
        let transfers = unsafe {
            let s = cast_slice::<u8, Transfer>(&backing_transfers);
            slice::from_raw_parts(s.as_ptr(), s.len())
        };
        let rtree = {
            RTree::bulk_load(
                stops
                    .iter()
                    .map(|stop| {
                        let latlng: LatLng = s2::cellid::CellID(stop.s2cell).into();
                        let location_cartesian =
                            lat_lng_to_cartesian(latlng.lat.deg(), latlng.lng.deg());
                        IndexedStop {
                            coords: location_cartesian,
                            id: stop.id(),
                        }
                    })
                    .collect(),
            )
        };

        let table = MmapTimetable {
            base_path,
            backing_routes,
            backing_route_stops,
            backing_route_trips,
            backing_stops,
            backing_stop_routes,
            backing_trip_stop_times,
            backing_transfer_index,
            backing_transfers,
            phantom: &PhantomData,
            client: reqwest::Client::new(),

            rtree,
            routes_slice: routes,
            route_stops_slice: route_stops,
            route_trips_slice: route_trips,
            stops_slice: stops,
            stop_routes_slice: stop_routes,
            trip_stop_times_slice: trip_stop_times,
            transfer_index_slice: transfer_index,
            transfers_slice: transfers,

            metadata_db,
        };
        Ok(table)
    }

    pub fn open(base_path: &PathBuf) -> Result<MmapTimetable<'a>, anyhow::Error> {
        debug!("Creating a new memory-mapped timetable. Opening files");
        debug!("Opening routes.");
        let routes = File::open(base_path.join("routes"))?;
        debug!("Opening route stops.");
        let route_stops = File::open(base_path.join("route_stops"))?;
        debug!("Opening route trips.");
        let route_trips = File::open(base_path.join("route_trips"))?;
        debug!("Opening stops.");
        let stops = File::open(base_path.join("stops"))?;
        debug!("Opening stop routes.");
        let stop_routes = File::open(base_path.join("stop_routes"))?;
        debug!("Opening stop times.");
        let trip_stop_times = File::open(base_path.join("trip_stop_times"))?;
        debug!("Opening transfer index.");
        let transfer_index = File::open(base_path.join("transfer_index"))?;
        debug!("Opening transfers.");
        let transfers = File::open(base_path.join("transfers"))?;

        debug!("Opening metadata database");
        let metadata_db = Database::open(base_path.join("metadata.db"))?;

        let page_bits = Some(21);

        debug!("mmapping");
        let backing_routes = unsafe { MmapOptions::new().huge(page_bits).map(&routes)? };
        let backing_route_stops = unsafe { MmapOptions::new().huge(page_bits).map(&route_stops)? };
        let backing_route_trips = unsafe { MmapOptions::new().huge(page_bits).map(&route_trips)? };
        let backing_stops = unsafe { MmapOptions::new().huge(page_bits).map(&stops)? };
        let backing_stop_routes = unsafe { MmapOptions::new().huge(page_bits).map(&stop_routes)? };
        let backing_trip_stop_times =
            unsafe { MmapOptions::new().huge(page_bits).map(&trip_stop_times)? };
        let backing_transfer_index =
            unsafe { MmapOptions::new().huge(page_bits).map(&transfer_index)? };
        let backing_transfers = unsafe { MmapOptions::new().huge(page_bits).map(&transfers)? };

        MmapTimetable::assemble(
            base_path.clone(),
            Pin::new(backing_routes),
            Pin::new(backing_route_stops),
            Pin::new(backing_route_trips),
            Pin::new(backing_stops),
            Pin::new(backing_stop_routes),
            Pin::new(backing_trip_stop_times),
            Pin::new(backing_transfer_index),
            Pin::new(backing_transfers),
            metadata_db,
        )
    }

    pub fn from_in_memory(
        in_memory_timetable: &InMemoryTimetableBuilder,
        base_path: &PathBuf,
    ) -> Result<MmapTimetable<'a>, anyhow::Error> {
        fs::create_dir_all(base_path)?;

        let in_memory_timetable = &in_memory_timetable.timetable;

        {
            // Drop all of these values before attempting to create a timetable from the raw data.
            {
                let routes = File::create(base_path.join("routes"))?;
                let route_stops = File::create(base_path.join("route_stops"))?;
                let route_trips = File::create(base_path.join("route_trips"))?;
                let stops = File::create(base_path.join("stops"))?;
                let stop_routes = File::create(base_path.join("stop_routes"))?;
                let trip_stop_times = File::create(base_path.join("trip_stop_times"))?;
                let _ = File::create(base_path.join("transfer_index"))?;
                let _ = File::create(base_path.join("transfers"))?;

                routes.set_len((size_of::<Route>() * in_memory_timetable.routes().len()) as u64)?;
                route_stops.set_len(
                    (size_of::<RouteStop>() * in_memory_timetable.route_stops().len()) as u64,
                )?;
                route_trips.set_len(
                    (size_of::<Trip>() * in_memory_timetable.route_trips().len()) as u64,
                )?;
                stops.set_len((size_of::<Stop>() * in_memory_timetable.stops().len()) as u64)?;
                stop_routes.set_len(
                    (size_of::<StopRoute>() * in_memory_timetable.stop_routes().len()) as u64,
                )?;
                trip_stop_times.set_len(
                    (size_of::<TripStopTime>() * in_memory_timetable.trip_stop_times().len())
                        as u64,
                )?;
            }

            let routes = File::options()
                .write(true)
                .read(true)
                .open(base_path.join("routes"))?;
            let route_stops = File::options()
                .write(true)
                .read(true)
                .open(base_path.join("route_stops"))?;
            let route_trips = File::options()
                .write(true)
                .read(true)
                .open(base_path.join("route_trips"))?;
            let stops = File::options()
                .write(true)
                .read(true)
                .open(base_path.join("stops"))?;
            let stop_routes = File::options()
                .write(true)
                .read(true)
                .open(base_path.join("stop_routes"))?;
            let trip_stop_times = File::options()
                .write(true)
                .read(true)
                .open(base_path.join("trip_stop_times"))?;

            let mut backing_routes = unsafe { MmapOptions::new().map_mut(&routes)? };
            let mut backing_route_stops = unsafe { MmapOptions::new().map_mut(&route_stops)? };
            let mut backing_route_trips = unsafe { MmapOptions::new().map_mut(&route_trips)? };
            let mut backing_stops = unsafe { MmapOptions::new().map_mut(&stops)? };
            let mut backing_stop_routes = unsafe { MmapOptions::new().map_mut(&stop_routes)? };
            let mut backing_trip_stop_times =
                unsafe { MmapOptions::new().map_mut(&trip_stop_times)? };

            backing_routes.copy_from_slice(cast_slice(in_memory_timetable.routes()));
            backing_route_stops.copy_from_slice(cast_slice(in_memory_timetable.route_stops()));
            backing_route_trips.copy_from_slice(cast_slice(in_memory_timetable.route_trips()));
            backing_stops.copy_from_slice(cast_slice(in_memory_timetable.stops()));
            backing_stop_routes.copy_from_slice(cast_slice(in_memory_timetable.stop_routes()));
            backing_trip_stop_times
                .copy_from_slice(cast_slice(in_memory_timetable.trip_stop_times()));

            let metadata_db = Database::create(base_path.join("metadata.db"))?;
            {
                let write = metadata_db.begin_write()?;
                {
                    let mut table = write.open_table(STOP_METADATA_TABLE)?;
                    for stop in in_memory_timetable.stops() {
                        let bytes = rmp_serde::to_vec(&in_memory_timetable.stop_metadata(stop))?;
                        table.insert(stop.id() as u64, bytes.as_slice())?;
                    }
                }
                write.commit()?;
            }
            {
                let write = metadata_db.begin_write()?;
                {
                    let mut table = write.open_table(TRIP_METADATA_TABLE)?;
                    for trip in in_memory_timetable.route_trips() {
                        let bytes = rmp_serde::to_vec(&in_memory_timetable.trip_metadata(trip))?;
                        table.insert(trip.trip_index as u64, bytes.as_slice())?;
                    }
                }
                write.commit()?;
            }
            {
                let write = metadata_db.begin_write()?;
                {
                    let mut table = write.open_table(ROUTE_SHAPE_TABLE)?;
                    for route in in_memory_timetable.routes() {
                        let bytes = rmp_serde::to_vec(&in_memory_timetable.route_shape(route))?;
                        table.insert(route.route_index as u64, bytes.as_slice())?;
                    }
                }
                write.commit()?;
            }
            info!("Done writing timetable");
        }
        MmapTimetable::open(base_path)
    }

    pub async fn concatenate<'b>(
        timetables: &[MmapTimetable<'b>],
        base_path: &PathBuf,
        valhalla_endpoint: Option<String>,
    ) -> MmapTimetable<'b> {
        {
            let total_routes: usize = timetables.iter().map(|tt| tt.routes().len()).sum();
            let total_route_stops: usize = timetables.iter().map(|tt| tt.route_stops().len()).sum();
            let total_route_trips: usize = timetables.iter().map(|tt| tt.route_trips().len()).sum();
            let total_stops: usize = timetables.iter().map(|tt| tt.stops().len()).sum();
            let total_stop_routes: usize = timetables.iter().map(|tt| tt.stop_routes().len()).sum();
            let total_trip_stop_times: usize =
                timetables.iter().map(|tt| tt.trip_stop_times().len()).sum();
            {
                let routes = File::create(base_path.join("routes")).unwrap();
                let route_stops = File::create(base_path.join("route_stops")).unwrap();
                let route_trips = File::create(base_path.join("route_trips")).unwrap();
                let stops = File::create(base_path.join("stops")).unwrap();
                let stop_routes = File::create(base_path.join("stop_routes")).unwrap();
                let trip_stop_times = File::create(base_path.join("trip_stop_times")).unwrap();

                routes
                    .set_len((size_of::<Route>() * total_routes) as u64)
                    .unwrap();
                route_stops
                    .set_len((size_of::<RouteStop>() * total_route_stops) as u64)
                    .unwrap();
                route_trips
                    .set_len((size_of::<Trip>() * total_route_trips) as u64)
                    .unwrap();
                stops
                    .set_len((size_of::<Stop>() * total_stops) as u64)
                    .unwrap();
                stop_routes
                    .set_len((size_of::<StopRoute>() * total_stop_routes) as u64)
                    .unwrap();
                trip_stop_times
                    .set_len((size_of::<TripStopTime>() * total_trip_stop_times) as u64)
                    .unwrap();
            }

            let routes = File::options()
                .write(true)
                .read(true)
                .open(base_path.join("routes"))
                .unwrap();
            let route_stops = File::options()
                .write(true)
                .read(true)
                .open(base_path.join("route_stops"))
                .unwrap();
            let route_trips = File::options()
                .write(true)
                .read(true)
                .open(base_path.join("route_trips"))
                .unwrap();
            let stops = File::options()
                .write(true)
                .read(true)
                .open(base_path.join("stops"))
                .unwrap();
            let stop_routes = File::options()
                .write(true)
                .read(true)
                .open(base_path.join("stop_routes"))
                .unwrap();
            let trip_stop_times = File::options()
                .write(true)
                .read(true)
                .open(base_path.join("trip_stop_times"))
                .unwrap();
            let _ = File::create(base_path.join("transfer_index")).unwrap();
            let _ = File::create(base_path.join("transfers")).unwrap();

            let mut backing_routes = unsafe { MmapOptions::new().map_mut(&routes).unwrap() };
            let mut backing_route_stops =
                unsafe { MmapOptions::new().map_mut(&route_stops).unwrap() };
            let mut backing_route_trips =
                unsafe { MmapOptions::new().map_mut(&route_trips).unwrap() };
            let mut backing_stops = unsafe { MmapOptions::new().map_mut(&stops).unwrap() };
            let mut backing_stop_routes =
                unsafe { MmapOptions::new().map_mut(&stop_routes).unwrap() };
            let mut backing_trip_stop_times =
                unsafe { MmapOptions::new().map_mut(&trip_stop_times).unwrap() };

            let mut route_cursor = 0usize;
            let mut route_stop_cursor = 0usize;
            let mut route_trip_cursor = 0usize;
            let mut stop_cursor = 0usize;
            let mut stop_route_cursor = 0usize;
            let mut trip_stop_time_cursor = 0usize;

            let route_slice: &mut [Route] = cast_slice_mut(&mut backing_routes);
            let route_stop_slice: &mut [RouteStop] = cast_slice_mut(&mut backing_route_stops);
            let route_trip_slice: &mut [Trip] = cast_slice_mut(&mut backing_route_trips);
            let stop_slice: &mut [Stop] = cast_slice_mut(&mut backing_stops);
            let stop_route_slice: &mut [StopRoute] = cast_slice_mut(&mut backing_stop_routes);
            let trip_stop_time_slice: &mut [TripStopTime] =
                cast_slice_mut(&mut backing_trip_stop_times);

            {
                // Make mutable copies of the slices.
                for tt in timetables {
                    let route_slice =
                        &mut route_slice[route_cursor..route_cursor + tt.routes().len()];
                    let route_stop_slice = &mut route_stop_slice
                        [route_stop_cursor..route_stop_cursor + tt.route_stops().len()];
                    let route_trip_slice = &mut route_trip_slice
                        [route_trip_cursor..route_trip_cursor + tt.route_trips().len()];
                    let stop_slice = &mut stop_slice[stop_cursor..stop_cursor + tt.stops().len()];
                    let stop_route_slice = &mut stop_route_slice
                        [stop_route_cursor..stop_route_cursor + tt.stop_routes().len()];
                    let trip_stop_time_slice = &mut trip_stop_time_slice
                        [trip_stop_time_cursor..trip_stop_time_cursor + tt.trip_stop_times().len()];

                    route_slice.copy_from_slice(tt.routes());
                    route_stop_slice.copy_from_slice(tt.route_stops());
                    route_trip_slice.copy_from_slice(tt.route_trips());
                    stop_slice.copy_from_slice(tt.stops());
                    stop_route_slice.copy_from_slice(tt.stop_routes());
                    trip_stop_time_slice.copy_from_slice(tt.trip_stop_times());

                    for route in route_slice {
                        route.first_route_stop += route_stop_cursor;
                        route.first_route_trip += route_trip_cursor;
                        route.route_index += route_cursor;
                    }
                    for route_stop in route_stop_slice {
                        route_stop.route_index += route_cursor;
                        route_stop.stop_index += stop_cursor;
                    }
                    for route_trip in route_trip_slice {
                        route_trip.trip_index += route_trip_cursor;
                        route_trip.route_index += route_cursor;
                        route_trip.first_trip_stop_time += trip_stop_time_cursor;
                        route_trip.last_trip_stop_time += trip_stop_time_cursor;
                    }
                    for stop in stop_slice {
                        stop.stop_index += stop_cursor;
                        stop.first_stop_route_index += stop_route_cursor;
                    }
                    for stop_route in stop_route_slice {
                        stop_route.route_index += route_cursor;
                    }
                    for trip_stop_time in trip_stop_time_slice {
                        trip_stop_time.trip_index += route_trip_cursor;
                    }

                    route_cursor += tt.routes().len();
                    route_stop_cursor += tt.route_stops().len();
                    route_trip_cursor += tt.route_trips().len();
                    stop_cursor += tt.stops().len();
                    stop_route_cursor += tt.stop_routes().len();
                    trip_stop_time_cursor += tt.trip_stop_times().len();
                }
            }
            let metadata_db = Database::create(base_path.join("metadata.db")).unwrap();
            {
                let write = metadata_db.begin_write().unwrap();
                {
                    let mut table = write.open_table(STOP_METADATA_TABLE).unwrap();
                    let mut cursor = 0usize;
                    for tt in timetables {
                        for stop in tt.stops() {
                            let bytes = rmp_serde::to_vec(&tt.stop_metadata(stop)).unwrap();
                            table
                                .insert((cursor + stop.id()) as u64, bytes.as_slice())
                                .unwrap();
                        }
                        cursor += tt.stops().len();
                    }
                }
                write.commit().unwrap();
            }
            {
                let write = metadata_db.begin_write().unwrap();
                {
                    let mut table = write.open_table(TRIP_METADATA_TABLE).unwrap();
                    let mut cursor = 0usize;
                    for tt in timetables {
                        for trip in tt.route_trips() {
                            let bytes = rmp_serde::to_vec(&tt.trip_metadata(trip)).unwrap();
                            table
                                .insert((cursor + trip.trip_index) as u64, bytes.as_slice())
                                .unwrap();
                        }
                        cursor += tt.route_trips().len();
                    }
                }
                write.commit().unwrap();
            }
            {
                let write = metadata_db.begin_write().unwrap();
                {
                    let mut table = write.open_table(ROUTE_SHAPE_TABLE).unwrap();
                    let mut cursor = 0usize;
                    for tt in timetables {
                        for route in tt.routes() {
                            let bytes = rmp_serde::to_vec(&tt.route_shape(route)).unwrap();
                            table
                                .insert((cursor + route.route_index) as u64, bytes.as_slice())
                                .unwrap();
                        }
                        cursor += tt.routes().len();
                    }
                }
                write.commit().unwrap();
            }
        }
        let mut tt = MmapTimetable::open(base_path).unwrap();
        tt.calculate_transfers(valhalla_endpoint).await.unwrap();
        tt
    }

    pub(crate) async fn calculate_transfers(
        &mut self,
        valhalla_endpoint: Option<String>,
    ) -> Result<(), Error> {
        {
            let mut rtree = RTree::<IndexedStop>::new();

            for (stop_id, stop) in self.stops().iter().enumerate() {
                let latlng: LatLng = s2::cellid::CellID(stop.s2cell).into();
                let location_cartesian = lat_lng_to_cartesian(latlng.lat.deg(), latlng.lng.deg());
                rtree.insert(IndexedStop {
                    coords: location_cartesian,
                    id: stop_id,
                });
            }
            self.rtree = rtree;
        }
        assert_eq!(self.stops().len(), self.rtree.size());

        debug!("Calculating transfer times");
        let transfers = {
            let transfers: Vec<_> = self
                .stops()
                .iter()
                .map(|from_stop| async {
                    self.calculate_transfer_matrix(from_stop, valhalla_endpoint.clone())
                        .await
                })
                .collect();
            let mut awaited_transfers = vec![];
            for transfer in transfers {
                awaited_transfers.push(transfer.await);
            }
            awaited_transfers
        };

        let transfer_index_file = File::options()
            .write(true)
            .read(true)
            .create(true)
            .open(&self.base_path.join("transfer_index"))?;
        transfer_index_file
            .set_len((size_of::<usize>() * transfers.len()) as u64)
            .unwrap();
        let transfer_file = File::options()
            .write(true)
            .read(true)
            .create(true)
            .open(&self.base_path.join("transfers"))?;
        transfer_file
            .set_len(
                transfers
                    .iter()
                    .map(|t| size_of::<Transfer>() * t.len())
                    .sum::<usize>() as u64,
            )
            .unwrap();

        let mut backing_transfer_index_mut =
            unsafe { MmapMut::map_mut(&transfer_index_file).unwrap() };
        let mut backing_transfers_mut = unsafe { MmapMut::map_mut(&transfer_file).unwrap() };

        let out_transfer_index = unsafe {
            let s = cast_slice_mut::<u8, usize>(&mut backing_transfer_index_mut);
            slice::from_raw_parts_mut(s.as_mut_ptr(), s.len())
        };
        let out_transfers = unsafe {
            let s = cast_slice_mut::<u8, Transfer>(&mut backing_transfers_mut);
            slice::from_raw_parts_mut(s.as_mut_ptr(), s.len())
        };

        let mut total_transfers_processed = 0;
        for (transfer_chunk_idx, transfers) in transfers.iter().enumerate() {
            out_transfer_index[transfer_chunk_idx] = total_transfers_processed;
            for transfer in transfers {
                out_transfers[total_transfers_processed] = *transfer;
                total_transfers_processed += 1;
            }
        }
        Ok(())
    }

    fn generate_transfer_candidates(&self, stop: &Stop) -> Vec<&Stop> {
        let latlng = stop.location();
        let mut transfer_candidates = vec![];
        for (count, (to_stop, dist_sq)) in self
            .rtree
            .nearest_neighbor_iter_with_distance_2(&lat_lng_to_cartesian(
                latlng.lat.deg(),
                latlng.lng.deg(),
            ))
            .enumerate()
        {
            let dist = dist_sq.sqrt();
            if dist > 5000f64 {
                break;
            } else if dist > 3000f64 && count > 50 {
                break;
            }
            transfer_candidates.push(self.stop(to_stop.id));
        }
        transfer_candidates
    }

    /// Calculate the transfer matrix using the given valhalla endpoint.
    async fn valhalla_transfer_matrix(
        &self,
        stop: &Stop,
        transfer_candidates: Vec<&Stop>,
        valhalla_endpoint: String,
    ) -> Vec<Transfer> {
        let latlng = stop.location();
        let request = MatrixRequest {
            sources: vec![ValhallaLocation {
                lat: latlng.lat.deg(),
                lon: latlng.lng.deg(),
            }],
            targets: transfer_candidates
                .iter()
                .map(|stop| ValhallaLocation {
                    lat: stop.location().lat.deg(),
                    lon: stop.location().lng.deg(),
                })
                .collect(),
            costing: "pedestrian".to_string(),
            matrix_locations: usize::max(transfer_candidates.len(), 25),
        };
        let transfer_matrix_response = matrix_request(&self.client, &valhalla_endpoint, request)
            .await
            .unwrap();

        let mut line_items = vec![];

        for line_item in &transfer_matrix_response.sources_to_targets[0] {
            let to_index = if let Some(to_index) = line_item.to_index {
                to_index
            } else {
                warn!("Invalid line item in valhalla response {:?}", line_item);
                continue;
            };
            let time = if let Some(time) = line_item.time {
                time
            } else {
                warn!("Invalid line item in valhalla response {:?}", line_item);
                continue;
            };

            line_items.push(Transfer {
                to: transfer_candidates[to_index].id(),
                from: stop.id(),
                time: time as u64,
            });
        }
        line_items
    }

    fn fake_walk_speed_transfer_matrix(
        &self,
        stop: &Stop,
        transfer_candidates: Vec<&Stop>,
    ) -> Vec<Transfer> {
        let latlng = stop.location();
        transfer_candidates
            .iter()
            .map(|to_stop| Transfer {
                to: to_stop.id(),
                from: stop.id(),
                time: (FAKE_WALK_SPEED_SECONDS_PER_METER
                    * latlng.distance(&to_stop.location()).rad()
                    * EARTH_RADIUS_APPROX) as u64,
            })
            .collect()
    }

    async fn calculate_transfer_matrix(
        &self,
        stop: &Stop,
        valhalla_endpoint: Option<String>,
    ) -> Vec<Transfer> {
        let transfer_candidates = self.generate_transfer_candidates(stop);
        if let Some(valhalla_endpoint) = valhalla_endpoint {
            self.valhalla_transfer_matrix(stop, transfer_candidates, valhalla_endpoint)
                .await
        } else {
            self.fake_walk_speed_transfer_matrix(stop, transfer_candidates)
        }
    }
}
