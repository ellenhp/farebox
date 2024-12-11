use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    marker::PhantomData,
    mem::size_of,
    path::PathBuf,
    pin::Pin,
    slice,
};

use anyhow::Ok;
use bytemuck::checked::cast_slice;
use log::debug;
use memmap2::{Mmap, MmapOptions};
use rstar::RTree;

use crate::raptor::geomath::IndexedStop;

use super::{
    in_memory::InMemoryTimetable, Route, RouteStop, Stop, StopRoute, Timetable, Transfer, Trip,
    TripMetadata, TripStopTime,
};

#[allow(unused)]
pub struct MmapTimetable<'a> {
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

    stop_metadata_map: HashMap<Stop, gtfs_structures::Stop>,
    trip_metadata_map: HashMap<Trip, TripMetadata>,

    phantom: &'a PhantomData<()>,
}

impl<'a> Timetable<'a> for MmapTimetable<'a> {
    #[inline]
    fn route(&'a self, route_id: usize) -> &Route {
        &self.routes()[route_id as usize]
    }

    #[inline]
    fn stop(&'a self, stop_id: usize) -> &Stop {
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
    fn stop_index(&'a self) -> &'a RTree<IndexedStop> {
        &self.rtree
    }

    fn stop_metadata(&'a self) -> &'a HashMap<Stop, gtfs_structures::Stop> {
        &self.stop_metadata_map
    }

    fn trip_metadata(&'a self) -> &'a HashMap<Trip, TripMetadata> {
        &self.trip_metadata_map
    }
}

impl<'a> MmapTimetable<'a> {
    fn assemble(
        backing_routes: Pin<Mmap>,
        backing_route_stops: Pin<Mmap>,
        backing_route_trips: Pin<Mmap>,
        backing_stops: Pin<Mmap>,
        backing_stop_routes: Pin<Mmap>,
        backing_trip_stop_times: Pin<Mmap>,
        backing_transfer_index: Pin<Mmap>,
        backing_transfers: Pin<Mmap>,
        rtree: RTree<IndexedStop>,
        stop_metadata: HashMap<Stop, gtfs_structures::Stop>,
        trip_metadata: HashMap<Trip, TripMetadata>,
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

        let table = MmapTimetable {
            backing_routes,
            backing_route_stops,
            backing_route_trips,
            backing_stops,
            backing_stop_routes,
            backing_trip_stop_times,
            backing_transfer_index,
            backing_transfers,
            phantom: &PhantomData,

            rtree,
            routes_slice: routes,
            route_stops_slice: route_stops,
            route_trips_slice: route_trips,
            stops_slice: stops,
            stop_routes_slice: stop_routes,
            trip_stop_times_slice: trip_stop_times,
            transfer_index_slice: transfer_index,
            transfers_slice: transfers,
            stop_metadata_map: stop_metadata,
            trip_metadata_map: trip_metadata,
        };
        Ok(table)
    }

    pub fn new(base_path: PathBuf) -> Result<MmapTimetable<'a>, anyhow::Error> {
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

        debug!("Opening stop metadata.");
        let stop_metadata = File::open(base_path.join("stop_metadata")).unwrap();
        let stop_metadata: HashMap<Stop, gtfs_structures::Stop> =
            rmp_serde::from_read(&stop_metadata)?;

        debug!("Opening trip metadata.");
        let trip_metadata = File::open(base_path.join("trip_metadata"))?;
        let trip_metadata: HashMap<Trip, TripMetadata> = rmp_serde::from_read(&trip_metadata)?;

        debug!("Opening rtree.");
        let rtree = File::open(base_path.join("rtree"))?;
        let rtree: RTree<IndexedStop> = rmp_serde::from_read(&rtree)?;

        debug!("mmapping");
        let backing_routes = unsafe { MmapOptions::new().map(&routes)? };
        let backing_route_stops = unsafe { MmapOptions::new().map(&route_stops)? };
        let backing_route_trips = unsafe { MmapOptions::new().map(&route_trips)? };
        let backing_stops = unsafe { MmapOptions::new().map(&stops)? };
        let backing_stop_routes = unsafe { MmapOptions::new().map(&stop_routes)? };
        let backing_trip_stop_times = unsafe { MmapOptions::new().map(&trip_stop_times)? };
        let backing_transfer_index = unsafe { MmapOptions::new().map(&transfer_index)? };
        let backing_transfers = unsafe { MmapOptions::new().map(&transfers)? };

        MmapTimetable::assemble(
            Pin::new(backing_routes),
            Pin::new(backing_route_stops),
            Pin::new(backing_route_trips),
            Pin::new(backing_stops),
            Pin::new(backing_stop_routes),
            Pin::new(backing_trip_stop_times),
            Pin::new(backing_transfer_index),
            Pin::new(backing_transfers),
            rtree,
            stop_metadata,
            trip_metadata,
        )
    }

    pub fn from_in_memory(
        in_memory_timetable: &InMemoryTimetable,
        base_path: &PathBuf,
    ) -> Result<MmapTimetable<'a>, anyhow::Error> {
        fs::create_dir_all(base_path)?;

        {
            // Drop all of these values before attempting to create a timetable from the raw data.
            {
                let routes = File::create(base_path.join("routes"))?;
                let route_stops = File::create(base_path.join("route_stops"))?;
                let route_trips = File::create(base_path.join("route_trips"))?;
                let stops = File::create(base_path.join("stops"))?;
                let stop_routes = File::create(base_path.join("stop_routes"))?;
                let trip_stop_times = File::create(base_path.join("trip_stop_times"))?;
                let transfer_index = File::create(base_path.join("transfer_index"))?;
                let transfers = File::create(base_path.join("transfers"))?;

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
                transfer_index.set_len(
                    (size_of::<usize>() * in_memory_timetable.transfer_index().len()) as u64,
                )?;
                transfers.set_len(
                    (size_of::<Transfer>() * in_memory_timetable.transfers().len()) as u64,
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
            let transfer_index = File::options()
                .write(true)
                .read(true)
                .open(base_path.join("transfer_index"))?;
            let transfers = File::options()
                .write(true)
                .read(true)
                .open(base_path.join("transfers"))?;

            let mut backing_routes = unsafe { MmapOptions::new().map_mut(&routes)? };
            let mut backing_route_stops = unsafe { MmapOptions::new().map_mut(&route_stops)? };
            let mut backing_route_trips = unsafe { MmapOptions::new().map_mut(&route_trips)? };
            let mut backing_stops = unsafe { MmapOptions::new().map_mut(&stops)? };
            let mut backing_stop_routes = unsafe { MmapOptions::new().map_mut(&stop_routes)? };
            let mut backing_trip_stop_times =
                unsafe { MmapOptions::new().map_mut(&trip_stop_times)? };
            let mut backing_transfer_index =
                unsafe { MmapOptions::new().map_mut(&transfer_index)? };
            let mut backing_transfers = unsafe { MmapOptions::new().map_mut(&transfers)? };

            backing_routes.copy_from_slice(cast_slice(in_memory_timetable.routes()));
            backing_route_stops.copy_from_slice(cast_slice(in_memory_timetable.route_stops()));
            backing_route_trips.copy_from_slice(cast_slice(in_memory_timetable.route_trips()));
            backing_stops.copy_from_slice(cast_slice(in_memory_timetable.stops()));
            backing_stop_routes.copy_from_slice(cast_slice(in_memory_timetable.stop_routes()));
            backing_trip_stop_times
                .copy_from_slice(cast_slice(in_memory_timetable.trip_stop_times()));
            backing_transfer_index
                .copy_from_slice(cast_slice(in_memory_timetable.transfer_index()));
            backing_transfers.copy_from_slice(cast_slice(in_memory_timetable.transfers()));

            {
                let mut rtree = File::create(base_path.join("rtree"))?;
                rtree.write_all(&rmp_serde::to_vec(in_memory_timetable.stop_index())?)?;
            }
            {
                let mut stop_metadata = File::create(base_path.join("stop_metadata"))?;
                stop_metadata.write_all(&rmp_serde::to_vec(
                    &in_memory_timetable.stop_metadata().clone(),
                )?)?;
            }
            {
                let mut trip_metadata = File::create(base_path.join("trip_metadata"))?;
                trip_metadata.write_all(&rmp_serde::to_vec(
                    &in_memory_timetable.trip_metadata().clone(),
                )?)?;
            }
        }
        MmapTimetable::new(base_path.clone())
    }
}
