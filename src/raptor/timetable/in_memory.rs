use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    f32, u32,
};

use anyhow::bail;
use chrono::{offset::LocalResult, DateTime, Days, Local, NaiveTime, TimeDelta, TimeZone};
use chrono_tz::Tz;
use gtfs_structures::{Agency, Gtfs, StopTime};
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

    stop_table: BTreeMap<StopKey, StopData>,
    route_index: BTreeMap<RouteKey, RouteId>,
    route_table: BTreeMap<RouteId, RouteData>,
}

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq, Ord)]
struct StopId(usize);

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq, Ord)]
struct RouteId(usize);

#[derive(Debug, Clone)]
struct TripInternal {
    service_day_start: DateTime<chrono_tz::Tz>,
    stop_times: Vec<StopTime>,
    gtfs_trip_id: String,
}

impl TripInternal {
    fn get_departure(&self) -> Option<DateTime<chrono_tz::Tz>> {
        let first_departure_time = self
            .stop_times
            .first()
            .map(|stop_time| stop_time.departure_time)??;
        if first_departure_time == u32::MAX {
            return None;
        }
        Some(
            self.service_day_start
                .checked_add_signed(TimeDelta::seconds(first_departure_time as i64))
                .expect("Failed to add departure time to service day start"),
        )
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
struct RouteKey {
    trip_stop_ids: Vec<String>,
    route_id: String,
}

#[derive(Debug, Clone)]
struct RouteData {
    id: RouteId,
    gtfs_route_id: String,
    shape: Option<Vec<ShapeCoordinate>>,
    trip_list: Vec<TripInternal>,
    stops: Vec<StopId>,
    shape_distances: Vec<f32>,
    agency_name: Option<String>,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
struct StopKey {
    gtfs_id: String,
}

#[derive(Debug, Clone)]
struct StopData {
    id: StopId,
    gtfs_id: String,
    stop_routes: BTreeSet<RouteId>,
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
            stop_table: BTreeMap::new(),
            route_index: BTreeMap::new(),
            route_table: BTreeMap::new(),
        };
        builder.preprocess_gtfs(gtfs)?;
        Ok(builder)
    }

    fn lookup_stop_data(&'a mut self, gtfs_id: &String) -> &'a mut StopData {
        let key = StopKey {
            gtfs_id: gtfs_id.clone(),
        };
        // If the stop isn't already in our table, add it.
        if !self.stop_table.contains_key(&key) {
            let stop_id = StopId(self.next_stop_id);
            self.next_stop_id += 1;
            self.stop_table.insert(
                key.clone(),
                StopData {
                    id: stop_id,
                    gtfs_id: gtfs_id.clone(),
                    stop_routes: BTreeSet::new(),
                },
            );
        }
        // Return a mutable reference.
        return self.stop_table.get_mut(&key).unwrap();
    }

    fn lookup_route_data(
        &'a mut self,
        gtfs: &Gtfs,
        trip: &gtfs_structures::Trip,
    ) -> &'a mut RouteData {
        // Farebox defines a "route" as something distinct from a GTFS route, because in GTFS there's no guarantee that a route always has the same stops in the same order. In fact, for bidirectional lines, the same route is usually used for trips in both directions, which violates RAPTOR's assumptions. To deal with this, we define a "route" as a set of trips that all visit the same stops in the same order and have the same GTFS route ID in the same GTFS feed.
        let trip_stop_ids: Vec<String> = trip
            .stop_times
            .iter()
            .map(|stop_time| stop_time.stop.id.clone())
            .collect();
        let gtfs_route_id = trip.route_id.clone();
        // If the `route_key` is shared between two trips, they belong to the same route.
        let route_key = RouteKey {
            trip_stop_ids,
            route_id: gtfs_route_id.clone(),
        };
        if !self.route_index.contains_key(&route_key) {
            let route_id = RouteId(self.next_route_id);
            self.next_route_id += 1;

            // Determine the path that the route travels.
            let shape: Option<Vec<ShapeCoordinate>> = if let Some(shape_id) = &trip.shape_id {
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

            // Determine the distance along the travel path of each stop.
            let shape_distances: Vec<f32> = trip
                .stop_times
                .iter()
                .map(|time| time.shape_dist_traveled.unwrap_or(f32::NAN))
                .collect();

            let stops = trip
                .stop_times
                .iter()
                .map(|stop_time| self.lookup_stop_data(&stop_time.stop.id).id.clone())
                .collect();

            // Determine the human-readable agency name.
            let agency_name = gtfs
                .agencies
                .iter()
                .find(|agency| {
                    agency.id
                        == gtfs
                            .get_route(&gtfs_route_id)
                            .expect("Trip's route ID not found in route table.")
                            .agency_id
                })
                .map(|agency| agency.name.clone());

            self.route_index.insert(route_key.clone(), route_id);
            self.route_table.insert(
                route_id,
                RouteData {
                    id: route_id,
                    gtfs_route_id,
                    shape,
                    trip_list: vec![],
                    stops,
                    shape_distances,
                    agency_name,
                },
            );

            self.route_table.get_mut(&route_id).unwrap()
        } else {
            // We already know of this route. Return a mutable reference.
            return self
                .route_table
                .get_mut(&self.route_index[&route_key])
                .unwrap();
        }
    }

    fn preprocess_gtfs(&mut self, gtfs: &Gtfs) -> Result<(), anyhow::Error> {
        let agencies: HashMap<String, &Agency> = gtfs
            .agencies
            .iter()
            .map(|agency| (agency.id.clone().unwrap_or(String::new()), agency))
            .collect();
        let start_date = Local::now().date_naive().pred_opt().unwrap();

        // First things first, go through every trip in the feed.
        for (gtfs_trip_id, trip) in &gtfs.trips {
            {
                let agency_id = if let Some(agency_id) = gtfs
                    .routes
                    .get(&trip.route_id)
                    .map(|route| route.agency_id.clone())
                    .flatten()
                {
                    agency_id
                } else {
                    continue;
                };
                let tz = self
                    .trip_agency_timezone(&agencies, &agency_id)
                    .expect("Failed to parse timezone");
                let route_data = self.lookup_route_data(gtfs, trip);
                let trip_days = gtfs.trip_days(&trip.service_id, start_date.clone());
                for day in trip_days {
                    if day <= 14 {
                        let date_time_offset = start_date
                            .checked_add_days(Days::new(day as u64))
                            .expect(&format!(
                                "Failed to add {day} days to date {:?}",
                                start_date
                            ));
                        // The start of a service day is defined as noon minus 12 hours.
                        let noon_service_day = match tz.from_local_datetime(
                            &date_time_offset.and_time(
                                NaiveTime::from_hms_opt(12, 0, 0)
                                    .expect("Failed to add 12 hours to service day"),
                            ),
                        ) {
                            LocalResult::Single(date_time) => date_time,
                            LocalResult::Ambiguous(a, _b) => {
                                // Pick one and call it good.
                                a
                            }
                            LocalResult::None => {
                                bail!("Gap in time (at noon? shouldn't be possible), can't determine service day start")
                            }
                        };
                        let service_day_start = noon_service_day
                            .checked_sub_signed(TimeDelta::hours(12))
                            .expect(
                                "Failed to subtract 12 hours from noon on the given service day.",
                            );

                        // Once we've assembled all the necessary data, push a trip to the route_data's trip_list for use later in `process_routes_trips`.
                        route_data.trip_list.push(TripInternal {
                            service_day_start,
                            stop_times: trip.stop_times.clone(),
                            gtfs_trip_id: gtfs_trip_id.clone(),
                        });
                    }
                }
            }
            let route_id = self.lookup_route_data(gtfs, trip).id;
            for stop_time in &trip.stop_times {
                self.lookup_stop_data(&stop_time.stop.id)
                    .stop_routes
                    .insert(route_id);
            }
        }

        debug!("Done sorting");

        self.process_routes_trips(gtfs)?;

        self.process_stops(gtfs)?;

        Result::Ok(())
    }

    fn process_routes_trips(&mut self, gtfs: &Gtfs) -> Result<(), anyhow::Error> {
        // TODO: How to deal with this route_table.clone()? It indicates an architectural problem IMO.
        for (_, route_data) in self.route_table.clone().iter() {
            let route = Route {
                route_index: route_data.id.0,
                first_route_stop: self.next_route_stop_id,
                first_route_trip: self.next_route_trip_id,
            };
            self.timetable
                .route_shapes
                .insert(route, route_data.shape.clone());
            self.timetable.routes.push(route);

            for (stop_seq, stop_id) in route_data.stops.iter().enumerate() {
                self.timetable.route_stops.push(RouteStop {
                    route_index: route_data.id.0,
                    stop_index: stop_id.0,
                    stop_seq: stop_seq as u32,
                    distance_along_route: route_data.shape_distances[stop_seq],
                });
                self.next_route_stop_id += 1;
            }
            let mut trips = route_data.trip_list.clone();
            trips.sort_by_cached_key(|trip| trip.get_departure());
            for trip in &trips {
                self.process_trip(gtfs, route_data, trip)?;
            }
        }
        Ok(())
    }

    fn trip_agency_timezone(
        &self,
        agencies: &HashMap<String, &Agency>,
        trip_agency_id: &String,
    ) -> Result<Tz, anyhow::Error> {
        let trip_agency = if let Some(agency) = agencies.get(trip_agency_id) {
            agency
        } else {
            if agencies.len() == 1 {
                agencies.values().next().unwrap()
            } else {
                bail!("No matching agency: {}, {:?}", trip_agency_id, agencies);
            }
        };
        Ok(trip_agency.timezone.parse().map_err(|_err| {
            anyhow::anyhow!(
                "Failed to parse tz for agency {}: {}",
                trip_agency,
                trip_agency.timezone
            )
        })?)
    }

    fn process_trip(
        &mut self,
        gtfs: &Gtfs,
        route_data: &RouteData,
        trip: &TripInternal,
    ) -> Result<(), anyhow::Error> {
        let first_trip_stop_time = self.next_trip_stop_time_id;

        #[cfg(feature = "enforce_invariants")]
        let mut prev_time = 0u32;
        for (stop_seq, stop_time) in trip.stop_times.iter().enumerate() {
            #[cfg(feature = "enforce_invariants")]
            if let Some(arrival_time) = stop_time.arrival_time {
                assert!(arrival_time >= prev_time);
                prev_time = arrival_time;
            }
            let arrival_time = trip
                .service_day_start
                .checked_add_signed(TimeDelta::seconds(
                    stop_time.arrival_time.unwrap_or(u32::MAX) as i64,
                ))
                .unwrap();
            let departure_time = trip
                .service_day_start
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
        let gtfs_trip = gtfs
            .get_trip(&trip.gtfs_trip_id)
            .expect("Trip not found in trip table.");
        let trip = Trip {
            trip_index: self.next_route_trip_id,
            route_index: route_data.id.0,
            first_trip_stop_time,
            last_trip_stop_time: self.next_trip_stop_time_id,
        };
        self.timetable.route_trips.push(trip);
        let metadata = TripMetadata {
            agency_name: route_data.agency_name.clone(),
            headsign: gtfs_trip.trip_headsign.clone(),
            route_name: gtfs.routes[&route_data.gtfs_route_id].short_name.clone(),
        };
        self.timetable.trip_metadata_map.insert(trip, metadata);

        self.next_route_trip_id += 1;

        Ok(())
    }

    fn process_stops(&mut self, gtfs: &Gtfs) -> Result<(), anyhow::Error> {
        let mut sorted_stops: Vec<&StopData> = self.stop_table.values().collect();
        sorted_stops.sort_by_cached_key(|stop_data| stop_data.id);
        for stop_data in sorted_stops {
            let gtfs_stop = gtfs.get_stop(&stop_data.gtfs_id).unwrap();
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
                stop_index: stop_data.id.0,
                s2cell: s2cell.0,
                first_stop_route_index: self.next_stop_route_id,
            };
            self.timetable.stops.push(stop);
            self.timetable
                .stop_metadata_map
                .insert(stop, gtfs_stop.clone());
            for route in &stop_data.stop_routes {
                let mut seq = 0usize;
                let mut found_seq = false;
                for route_stop_seq_candidate in &self.route_table[&route].stops {
                    if &stop_data.id == route_stop_seq_candidate {
                        found_seq = true;
                        break;
                    }
                    seq += 1;
                }
                assert!(found_seq);
                self.timetable.stop_routes.push(StopRoute {
                    route_index: route.0,
                    stop_seq: seq,
                });
                self.next_stop_route_id += 1;
            }
        }
        Ok(())
    }
}
