use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    f32, u32,
};

use anyhow::bail;
use chrono::{Days, Local, NaiveDate, TimeDelta, TimeZone};
use chrono_tz::Tz;
use gtfs_structures::{Agency, Gtfs};
use log::{debug, warn};
use rstar::RTree;
use s2::{cellid::CellID, latlng::LatLng};

use crate::raptor::{
    geomath::IndexedStop,
    timetable::{Route, RouteStop, Stop, StopRoute, Transfer, Trip, TripStopTime},
};

use super::{ShapeCoordinate, Timetable, TripMetadata};

#[derive(Debug, Clone)]
#[repr(C)]
pub(crate) struct InMemoryTimetable {
    routes: Vec<Route>,
    route_stops: Vec<RouteStop>,
    route_trips: Vec<Trip>,
    stops: Vec<Stop>,
    stop_routes: Vec<StopRoute>,
    trip_stop_times: Vec<TripStopTime>,
    transfer_index: Vec<usize>,
    transfers: Vec<Transfer>,
    trip_metadata_map: HashMap<Trip, TripMetadata>,
    stop_metadata_map: HashMap<Stop, gtfs_structures::Stop>,
    route_shapes: HashMap<Route, Option<Vec<ShapeCoordinate>>>,
}

impl<'a> Timetable<'a> for InMemoryTimetable {
    #[inline]
    fn route(&'a self, route_id: usize) -> &'a Route {
        &self.routes[route_id as usize]
    }

    #[inline]
    fn stop(&'a self, stop_id: usize) -> &'a Stop {
        &self.stops[stop_id as usize]
    }

    #[inline]
    fn transfers_from(&'a self, stop_id: usize) -> &'a [Transfer] {
        Transfer::all_transfers(self.stop(stop_id), self)
    }

    #[inline]
    fn stop_count(&self) -> usize {
        self.stops.len()
    }

    #[inline]
    fn stops(&'a self) -> &'a [Stop] {
        &self.stops
    }

    #[inline]
    fn stop_routes(&'a self) -> &'a [StopRoute] {
        &self.stop_routes
    }

    #[inline]
    fn routes(&'a self) -> &'a [Route] {
        &self.routes
    }

    #[inline]
    fn route_stops(&'a self) -> &'a [RouteStop] {
        &self.route_stops
    }

    #[inline]
    fn route_trips(&'a self) -> &'a [Trip] {
        &self.route_trips
    }

    #[inline]
    fn trip_stop_times(&'a self) -> &'a [TripStopTime] {
        &self.trip_stop_times
    }

    #[inline]
    fn transfers(&'a self) -> &'a [Transfer] {
        &self.transfers
    }

    #[inline]
    fn transfer_index(&'a self) -> &'a [usize] {
        &self.transfer_index
    }

    fn stop_metadata(&'a self, stop: &Stop) -> gtfs_structures::Stop {
        self.stop_metadata_map[stop].clone()
    }

    fn trip_metadata(&'a self, trip: &Trip) -> TripMetadata {
        self.trip_metadata_map[trip].clone()
    }

    fn stop_index_copy(&'a self) -> RTree<IndexedStop> {
        RTree::new()
    }

    fn nearest_stops(&'a self, _lat: f64, _lng: f64, _n: usize) -> Vec<(&'a Stop, f64)> {
        Vec::new()
    }

    fn route_shape(&'a self, route: &Route) -> Option<Vec<ShapeCoordinate>> {
        self.route_shapes[route].clone()
    }
}

impl<'a> InMemoryTimetable {
    pub(crate) fn new() -> InMemoryTimetable {
        InMemoryTimetable {
            routes: vec![],
            route_stops: vec![],
            route_trips: vec![],
            stops: vec![],
            stop_routes: vec![],
            trip_stop_times: vec![],
            transfer_index: vec![],
            transfers: vec![],
            trip_metadata_map: HashMap::new(),
            stop_metadata_map: HashMap::new(),
            route_shapes: HashMap::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InMemoryTimetableBuilderError {
    #[error("")]
    ParseError(String),
}

#[derive(Debug)]
pub struct InMemoryTimetableBuilder {
    next_stop_id: usize,
    next_stop_route_id: usize,
    next_route_id: usize,
    next_route_trip_id: usize,
    next_route_stop_id: usize,
    next_trip_stop_time_id: usize,
    pub(crate) timetable: InMemoryTimetable,
}

impl<'a> InMemoryTimetableBuilder {
    pub fn new(gtfs: &Gtfs) -> Result<Self, anyhow::Error> {
        let mut builder = InMemoryTimetableBuilder {
            next_stop_id: 0,
            next_stop_route_id: 0,
            next_route_id: 0,
            next_route_trip_id: 0,
            next_route_stop_id: 0,
            next_trip_stop_time_id: 0,
            timetable: InMemoryTimetable::new(),
        };
        builder.preprocess_gtfs(gtfs)?;
        Ok(builder)
    }

    fn preprocess_gtfs(&mut self, gtfs: &Gtfs) -> Result<(), anyhow::Error> {
        let agencies: HashMap<String, &Agency> = gtfs
            .agencies
            .iter()
            .map(|agency| (agency.id.clone().unwrap_or(String::new()), agency))
            .collect();
        let start_date = Local::now().date_naive().pred_opt().unwrap();

        let mut stop_to_stop_id_map = BTreeMap::new();
        for (gtfs_stop_id, _stop) in &gtfs.stops {
            stop_to_stop_id_map.insert(gtfs_stop_id.clone(), self.next_stop_id);
            self.next_stop_id += 1;
        }

        let mut route_to_route_id: BTreeMap<(Vec<usize>, String), (usize, Vec<f32>)> =
            BTreeMap::new();
        let mut route_shapes: BTreeMap<usize, Option<Vec<ShapeCoordinate>>> = BTreeMap::new();
        let mut route_id_to_trip_list: BTreeMap<usize, Vec<(u16, String)>> = BTreeMap::new();
        let mut stop_id_to_route_list: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
        for (gtfs_trip_id, trip) in &gtfs.trips {
            let trip_stops: Vec<usize> = trip
                .stop_times
                .iter()
                .map(|time| *stop_to_stop_id_map.get(&time.stop.id).unwrap())
                .collect();
            let trip_shape_distances: Vec<f32> = gtfs
                .get_trip(&gtfs_trip_id)
                .unwrap()
                .stop_times
                .iter()
                .map(|time| time.shape_dist_traveled.unwrap_or(f32::NAN))
                .collect();
            let route_id = if let Some((id, _)) =
                route_to_route_id.get(&(trip_stops.clone(), trip.route_id.clone()))
            {
                *id
            } else {
                let id = self.next_route_id;
                route_to_route_id.insert(
                    (trip_stops.clone(), trip.route_id.clone()),
                    (id, trip_shape_distances),
                );
                route_id_to_trip_list.insert(id, vec![]);

                let polyline: Option<Vec<ShapeCoordinate>> = if let Some(shape_id) = &trip.shape_id
                {
                    if let Ok(coords) = gtfs.get_shape(&shape_id) {
                        Some(
                            coords
                                .iter()
                                .map(|coord| ShapeCoordinate {
                                    lat: coord.latitude,
                                    lon: coord.longitude,
                                    distance_along_shape: coord.dist_traveled,
                                })
                                .collect(),
                        )
                    } else {
                        warn!("Could not look up shape {shape_id}");
                        None
                    }
                } else {
                    None
                };
                route_shapes.insert(id, polyline);
                self.next_route_id += 1;
                id
            };
            for stop in &trip_stops {
                if let Some(route_list) = stop_id_to_route_list.get_mut(&stop) {
                    route_list.insert(route_id);
                } else {
                    let mut set: BTreeSet<usize> = BTreeSet::new();
                    set.insert(route_id);
                    stop_id_to_route_list.insert(*stop, set);
                }
            }

            let trip_days = gtfs.trip_days(&trip.service_id, start_date.clone());

            for day in trip_days {
                if day <= 7 {
                    route_id_to_trip_list
                        .get_mut(&route_id)
                        .unwrap()
                        .push((day, gtfs_trip_id.clone()));
                }
            }
        }
        // Handy to have this sorted already. Maps internal stop ID to a gtfs-internal stop ID.
        let stop_id_to_stop_map: BTreeMap<usize, String> = stop_to_stop_id_map
            .iter()
            .map(|(k, v)| (v.clone(), k.clone()))
            .collect();

        assert_eq!(stop_id_to_stop_map.len(), stop_to_stop_id_map.len());

        // Handy to have this sorted already. Maps internal route ID to a sequence of internal stop IDs.
        let route_id_to_route_map: BTreeMap<usize, Vec<usize>> = route_to_route_id
            .iter()
            .map(|((k, _), (v, _))| (v.clone(), k.clone()))
            .collect();
        let route_id_to_stop_distances: BTreeMap<usize, Vec<f32>> = route_to_route_id
            .iter()
            .map(|((_, _), (v, k))| (v.clone(), k.clone()))
            .collect();

        assert_eq!(route_to_route_id.len(), route_id_to_route_map.len());

        debug!("Done sorting");

        self.process_routes_trips(
            gtfs,
            start_date,
            &agencies,
            &route_id_to_route_map,
            &route_id_to_stop_distances,
            &route_id_to_trip_list,
            &route_shapes,
        );

        self.process_stops(
            gtfs,
            &route_id_to_route_map,
            &stop_id_to_stop_map,
            &stop_id_to_route_list,
        )?;

        Result::Ok(())
    }

    fn process_routes_trips(
        &mut self,
        gtfs: &Gtfs,
        start_date: NaiveDate,
        agencies: &HashMap<String, &Agency>,
        route_id_to_route_map: &BTreeMap<usize, Vec<usize>>,
        route_id_to_stop_distances: &BTreeMap<usize, Vec<f32>>,
        route_id_to_trip_list: &BTreeMap<usize, Vec<(u16, String)>>,
        route_shapes: &BTreeMap<usize, Option<Vec<ShapeCoordinate>>>,
    ) {
        for (route_id, route_stop_list) in route_id_to_route_map.iter() {
            let route = Route {
                route_index: *route_id,
                first_route_stop: self.next_route_stop_id,
                first_route_trip: self.next_route_trip_id,
            };
            self.timetable
                .route_shapes
                .insert(route, route_shapes[route_id].clone());
            self.timetable.routes.push(route);

            for (stop_seq, stop_id) in route_stop_list.iter().enumerate() {
                self.timetable.route_stops.push(RouteStop {
                    route_index: *route_id,
                    stop_index: *stop_id,
                    stop_seq: stop_seq as u32,
                    distance_along_route: route_id_to_stop_distances[route_id][stop_seq],
                });
                self.next_route_stop_id += 1;
            }
            let trips_pre_sort = route_id_to_trip_list.get(&route_id).unwrap().clone();
            let mut trips = trips_pre_sort.clone();
            trips.sort_by_cached_key(|(day, gtfs_trip_id)| {
                gtfs.get_trip(gtfs_trip_id)
                    .unwrap()
                    .stop_times
                    .iter()
                    .filter_map(|stop_time| stop_time.departure_time)
                    .next()
                    .unwrap_or(u32::MAX)
                    // TODO: This is a DST bug.
                    .saturating_add(*day as u32 * 3600 * 24)
            });
            for (day, gtfs_trip_id) in &trips {
                let trip_agency_id = gtfs.routes[&gtfs.trips[gtfs_trip_id].route_id]
                    .agency_id
                    .clone()
                    .unwrap_or(String::new());
                let trip_agency = if let Some(agency) = agencies.get(&trip_agency_id) {
                    agency
                } else {
                    if agencies.len() == 1 {
                        agencies.values().next().unwrap()
                    } else {
                        warn!("No matching agency: {}, {:?}", trip_agency_id, agencies);
                        continue;
                    }
                };
                let agency_tz: Tz = trip_agency.timezone.parse().unwrap();
                let first_trip_stop_time = self.next_trip_stop_time_id;

                let trip = gtfs.get_trip(gtfs_trip_id).unwrap();

                #[cfg(feature = "enforce_invariants")]
                let mut prev_time = 0u32;
                for (stop_seq, stop_time) in trip.stop_times.iter().enumerate() {
                    #[cfg(feature = "enforce_invariants")]
                    if let Some(arrival_time) = stop_time.arrival_time {
                        assert!(arrival_time >= prev_time);
                        prev_time = arrival_time;
                    }
                    let day_start = agency_tz
                        .from_local_datetime(
                            &start_date
                                .checked_add_days(Days::new(*day as u64))
                                .unwrap()
                                // GTFS "Time" fields are measured from noon minus 12hrs.
                                .and_hms_opt(12, 0, 0)
                                .unwrap(),
                        )
                        .unwrap()
                        // GTFS "Time" fields are measured from noon minus 12hr.
                        .checked_sub_signed(TimeDelta::hours(12))
                        .unwrap();

                    let arrival_time = day_start
                        .checked_add_signed(TimeDelta::seconds(
                            stop_time.arrival_time.unwrap_or(u32::MAX) as i64,
                        ))
                        .unwrap();
                    let departure_time = day_start
                        .checked_add_signed(TimeDelta::seconds(
                            stop_time.departure_time.unwrap_or(u32::MAX) as i64,
                        ))
                        .unwrap();
                    self.timetable.trip_stop_times.push(TripStopTime::new(
                        self.next_route_trip_id,
                        stop_seq,
                        arrival_time,
                        departure_time,
                    ));
                    self.next_trip_stop_time_id += 1;
                }
                let trip = Trip {
                    trip_index: self.next_route_trip_id,
                    route_index: *route_id,
                    first_trip_stop_time,
                    last_trip_stop_time: self.next_trip_stop_time_id,
                };
                self.timetable.route_trips.push(trip);
                let agency_name = trip_agency.name.clone();
                let metadata = TripMetadata {
                    agency_name: Some(agency_name),
                    headsign: gtfs.trips[gtfs_trip_id].clone().trip_headsign,
                    route_name: gtfs.routes[&gtfs.trips[gtfs_trip_id].route_id]
                        .short_name
                        .clone(),
                };
                self.timetable.trip_metadata_map.insert(trip, metadata);

                self.next_route_trip_id += 1;
            }
        }
    }

    fn process_stops(
        &mut self,
        gtfs: &Gtfs,
        route_id_to_route_map: &BTreeMap<usize, Vec<usize>>,
        stop_id_to_stop_map: &BTreeMap<usize, String>,
        stop_id_to_route_list: &BTreeMap<usize, BTreeSet<usize>>,
    ) -> Result<(), anyhow::Error> {
        for (stop_id, gtfs_stop_id) in stop_id_to_stop_map.iter() {
            let gtfs_stop = gtfs.get_stop(&gtfs_stop_id).unwrap();
            let lat = if let Some(lat) = gtfs_stop.latitude {
                lat
            } else {
                bail!("Can't process feeds containing stop IDs without lat/lng")
            };
            let lng = if let Some(lng) = gtfs_stop.longitude {
                lng
            } else {
                bail!("Can't process feeds containing stop IDs without lat/lng")
            };
            let s2cell: CellID = LatLng::from_degrees(lat, lng).into();
            let stop = Stop {
                stop_index: *stop_id,
                s2cell: s2cell.0,
                first_stop_route_index: self.next_stop_route_id,
            };
            self.timetable.stops.push(stop);
            self.timetable
                .stop_metadata_map
                .insert(stop, gtfs_stop.clone());
            for route in stop_id_to_route_list
                .get(&stop_id)
                .unwrap_or(&BTreeSet::new())
            {
                let mut seq = 0usize;
                let mut found_seq = false;
                for route_stop_seq_candidate in route_id_to_route_map.get(route).unwrap() {
                    if stop_id == route_stop_seq_candidate {
                        found_seq = true;
                        break;
                    }
                    seq += 1;
                }
                assert!(found_seq);
                self.timetable.stop_routes.push(StopRoute {
                    route_index: *route,
                    stop_seq: seq,
                });
                self.next_stop_route_id += 1;
            }
        }
        Ok(())
    }
}
