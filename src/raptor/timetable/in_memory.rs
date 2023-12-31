use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::{Duration, Utc};
use chrono_tz::Tz;
use gtfs_structures::{Calendar, Gtfs};
use log::{info, warn};
use reqwest::Client;
use rstar::RTree;
use s2::{cellid::CellID, latlng::LatLng};

use crate::{
    raptor::{
        geomath::{lat_lng_to_cartesian, IndexedStop, EARTH_RADIUS_APPROX},
        timetable::{Route, RouteStop, Stop, StopRoute, Transfer, Trip, TripStopTime},
    },
    valhalla::{matrix_request, MatrixRequest, ValhallaLocation},
};

use super::{Timetable, TripMetadata};

#[derive(Debug, Clone)]
#[repr(C)]
pub struct InMemoryTimetable {
    routes: Vec<Route>,
    route_stops: Vec<RouteStop>,
    route_trips: Vec<Trip>,
    stops: Vec<Stop>,
    stop_routes: Vec<StopRoute>,
    trip_stop_times: Vec<TripStopTime>,
    transfer_index: Vec<usize>,
    transfers: Vec<Transfer>,
    rtree: RTree<IndexedStop>,
    trip_metadata_map: HashMap<Trip, TripMetadata>,
    stop_metadata_map: HashMap<Stop, gtfs_structures::Stop>,
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

impl<'a> InMemoryTimetable {
    pub fn new() -> InMemoryTimetable {
        InMemoryTimetable {
            routes: vec![],
            route_stops: vec![],
            route_trips: vec![],
            stops: vec![],
            stop_routes: vec![],
            trip_stop_times: vec![],
            transfer_index: vec![],
            transfers: vec![],
            rtree: RTree::new(),
            trip_metadata_map: HashMap::new(),
            stop_metadata_map: HashMap::new(),
        }
    }

    pub async fn from_gtfs(
        gtfs: &[Gtfs],
        valhalla_endpoint: &str,
        estimate_transfers: bool,
    ) -> InMemoryTimetable {
        let in_memory_timetable = InMemoryTimetable::new();
        let timetable = {
            let mut in_memory_timetable_builder =
                InMemoryTimetableBuilder::new(in_memory_timetable, valhalla_endpoint);
            for gtfs in gtfs {
                in_memory_timetable_builder
                    .preprocess_gtfs(gtfs, estimate_transfers)
                    .await
                    .unwrap();
            }
            in_memory_timetable_builder.to_timetable()
        };
        timetable
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InMemoryTimetableBuilderError {
    #[error("")]
    ParseError(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct FeedUniqueKey {
    feed: usize,
    key: String,
}

#[derive(Debug)]
pub struct InMemoryTimetableBuilder {
    next_feed_id: usize,
    next_service_day_id: usize,
    next_route_id: usize,
    next_trip_id: usize,
    next_stop_id: usize,
    timetable: InMemoryTimetable,
    valhalla_endpoint: String,
}

impl<'a> InMemoryTimetableBuilder {
    fn new(timetable: InMemoryTimetable, valhalla_endpoint: &str) -> InMemoryTimetableBuilder {
        InMemoryTimetableBuilder {
            next_feed_id: 0,
            next_service_day_id: 0,
            next_route_id: 0,
            next_trip_id: 0,
            next_stop_id: 0,
            timetable,
            valhalla_endpoint: valhalla_endpoint.to_string(),
        }
    }

    fn get_gtfs_timezone(gtfs: &Gtfs) -> Result<Tz, anyhow::Error> {
        Ok(gtfs.agencies[0]
            .timezone
            .parse()
            .map_err(|e: String| InMemoryTimetableBuilderError::ParseError(e))?)
    }

    fn verify_service_offered(gtfs: &Gtfs, calendar: &Calendar) -> Result<bool, anyhow::Error> {
        let timezone = InMemoryTimetableBuilder::get_gtfs_timezone(gtfs)?;
        let current_time_local = Utc::now().with_timezone(&timezone);
        let time_since_start = current_time_local
            .date_naive()
            .signed_duration_since(calendar.start_date);
        let time_until_end = calendar
            .end_date
            .signed_duration_since(current_time_local.date_naive());
        if time_since_start < Duration::zero() || time_until_end < Duration::zero() {
            Ok(false)
        } else {
            Ok(true)
        }
    }

    pub async fn preprocess_gtfs(
        &mut self,
        gtfs: &Gtfs,
        estimate_transfers: bool,
    ) -> Result<(), anyhow::Error> {
        let gtfs_feed_id = self.next_feed_id;
        self.next_feed_id += 1;

        let mut stop_to_stop_id_map = BTreeMap::new();
        for (gtfs_stop_id, _stop) in &gtfs.stops {
            stop_to_stop_id_map.insert(gtfs_stop_id.clone(), dbg!(stop_to_stop_id_map.len()));
        }
        dbg!(stop_to_stop_id_map.len());

        let mut route_to_route_id: BTreeMap<Vec<usize>, usize> = BTreeMap::new();
        let mut stop_to_route: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        let mut route_id_to_trip_list: BTreeMap<usize, Vec<(usize, String)>> = BTreeMap::new();
        let mut stop_id_to_route_list: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
        for (gtfs_trip_id, trip) in &gtfs.trips {
            let trip_stops: Vec<usize> = trip
                .stop_times
                .iter()
                .map(|time| *stop_to_stop_id_map.get(&time.stop.id).unwrap())
                .collect();
            let route_id = if let Some(id) = route_to_route_id.get(&trip_stops) {
                *id
            } else {
                let id = self.next_route_id;
                let old_size = route_to_route_id.len();
                route_to_route_id.insert(trip_stops.clone(), id);
                assert_ne!(old_size, route_to_route_id.len());
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

            let this_trip_id = self.next_trip_id;
            self.next_trip_id += 1;

            if let Some(trip_list) = route_id_to_trip_list.get_mut(&route_id) {
                trip_list.push((this_trip_id, gtfs_trip_id.clone()));
            } else {
                route_id_to_trip_list.insert(route_id, vec![(this_trip_id, gtfs_trip_id.clone())]);
            }

            for stop_time in &trip.stop_times {
                if let Some(routes) = stop_to_route.get_mut(&stop_time.stop.id) {
                    routes.push(route_id);
                } else {
                    stop_to_route.insert(stop_time.stop.id.clone(), vec![route_id]);
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
            .map(|(k, v)| (v.clone() as usize, k.clone()))
            .collect();

        assert_eq!(route_to_route_id.len(), route_id_to_route_map.len());

        {
            let mut total_route_stops = 0usize;
            let mut total_route_trips = 0usize;
            let mut total_trip_stop_times = 0usize;
            for (route_id, route_stop_list) in route_id_to_route_map.iter() {
                let route = Route {
                    route_index: *route_id,
                    first_route_stop: total_route_stops,
                    first_route_trip: total_route_trips,
                };
                self.timetable.routes.push(route);
                // TODO: Feed ID.

                for (stop_seq, stop_id) in route_stop_list.iter().enumerate() {
                    self.timetable.route_stops.push(RouteStop {
                        route_index: *route_id,
                        stop_index: *stop_id,
                        stop_seq,
                    });
                    total_route_stops += 1;
                }
                let trips_pre_sort = route_id_to_trip_list.get(&route_id).unwrap().clone();
                let mut trips = trips_pre_sort.clone();
                trips.sort_by_cached_key(|(_trip_seq, gtfs_trip_id)| {
                    gtfs.get_trip(gtfs_trip_id).unwrap().stop_times[0]
                        .departure_time
                        .unwrap()
                });
                for (_trip_id, gtfs_trip_id) in &trips {
                    let first_trip_stop_time = total_trip_stop_times;

                    for (stop_seq, stop_time) in gtfs
                        .get_trip(gtfs_trip_id)
                        .unwrap()
                        .stop_times
                        .iter()
                        .enumerate()
                    {
                        self.timetable.trip_stop_times.push(TripStopTime {
                            trip_index: total_route_trips,
                            route_stop_seq: stop_seq,
                            arrival_time: stop_time.arrival_time.unwrap(),
                            departure_time: stop_time.departure_time.unwrap(),
                        });
                        total_trip_stop_times += 1;
                    }
                    let trip = Trip {
                        trip_index: total_route_trips,
                        route_index: *route_id,
                        first_trip_stop_time,
                        last_trip_stop_time: total_trip_stop_times,
                    };
                    self.timetable.route_trips.push(trip);
                    let metadata = TripMetadata {
                        headsign: gtfs.trips[gtfs_trip_id].clone().trip_headsign,
                        route_name: gtfs.routes[&gtfs.trips[gtfs_trip_id].route_id]
                            .short_name
                            .clone(),
                    };
                    self.timetable.trip_metadata_map.insert(trip, metadata);

                    total_route_trips += 1;
                }
            }
        }

        {
            let mut total_stops = 0usize;
            let mut total_stop_routes = 0usize;

            for (stop_seq, (stop_id, gtfs_stop_id)) in stop_id_to_stop_map.iter().enumerate() {
                assert_eq!(stop_seq, *stop_id);
                assert_eq!(stop_seq, total_stops);
                drop(stop_seq);

                let gtfs_stop = gtfs.get_stop(&gtfs_stop_id).unwrap();
                let lat = gtfs_stop.latitude.expect("Unknown location");
                let lng = gtfs_stop.longitude.expect("Unknown location");
                let s2cell: CellID = LatLng::from_degrees(lat, lng).into();
                let stop = Stop {
                    stop_index: *stop_id,
                    s2cell: s2cell.0,
                    first_stop_route_index: total_stop_routes,
                };
                self.timetable.stops.push(stop);
                self.timetable
                    .stop_metadata_map
                    .insert(stop, gtfs_stop.clone());
                // TODO: Feed ID.
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
                    total_stop_routes += 1;
                }
                let location_cartesian = lat_lng_to_cartesian(lat, lng);
                self.timetable.rtree.insert(IndexedStop {
                    coords: location_cartesian,
                    id: *stop_id,
                });
                total_stops += 1;
            }
        }

        let client = Client::new();
        let rtree = self.timetable.rtree.clone();

        info!("Calculating transfer times");
        let transfers = {
            let transfers: Vec<_> = self
                .timetable
                .stops
                .iter()
                .map(|from_stop| {
                    let latlng = from_stop.location();
                    let mut transfer_candidates = vec![];
                    for (count, (to_stop, dist_sq)) in rtree
                        .nearest_neighbor_iter_with_distance_2(&lat_lng_to_cartesian(
                            latlng.lat.deg(),
                            latlng.lng.deg(),
                        ))
                        .enumerate()
                    {
                        let dist = dist_sq.sqrt();
                        if dist > 1000f64 {
                            break;
                        }
                        if count > 50 {
                            break;
                        }
                        transfer_candidates.push(self.timetable.stop(to_stop.id));
                    }
                    (
                        from_stop,
                        transfer_candidates,
                        client.clone(),
                        self.valhalla_endpoint.clone(),
                    )
                })
                .map(
                    |(from_stop, transfer_candidates, client, valhalla_endpoint)| async move {
                        let latlng = from_stop.location();
                        if estimate_transfers {
                            transfer_candidates
                                .iter()
                                .map(|to_stop| Transfer {
                                    to: to_stop.id(),
                                    from: from_stop.id(),
                                    time: (latlng.distance(&to_stop.location()).rad()
                                        * EARTH_RADIUS_APPROX)
                                        as u64, // 1 meter per second.
                                })
                                .collect()
                        } else {
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
                            let transfer_matrix_response =
                                matrix_request(&client, &valhalla_endpoint, request)
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
                                    from: from_stop.id(),
                                    time: time as u64,
                                });
                            }
                            line_items
                        }
                    },
                )
                .collect();
            let mut awaited_transfers = vec![];
            for transfer in transfers {
                awaited_transfers.push(transfer.await);
            }
            awaited_transfers
        };
        for transfers in transfers {
            self.timetable
                .transfer_index
                .push(self.timetable.transfers.len());
            for transfer in transfers {
                self.timetable.transfers.push(transfer);
            }
        }
        Result::Ok(())
    }

    fn to_timetable(self) -> InMemoryTimetable {
        self.timetable.clone()
    }
}
