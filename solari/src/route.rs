use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use geo::ClosestPoint;
use geo_types::{Coord, Line, LineString, Point};
use log::{debug, info};
use s2::latlng::LatLng;
use serde::Serialize;
use solari_geomath::EARTH_RADIUS_APPROX;
use solari_spatial::SphereIndexMmap;
use solari_transfers::{fast_paths::FastGraphStatic, TransferGraph, TransferGraphSearcher};
use time::OffsetDateTime;

use crate::{
    api::{
        response::{ResponseStatus, SolariResponse},
        SolariItinerary, SolariLeg,
    },
    raptor::timetable::TripStopTime,
    spatial::FAKE_WALK_SPEED_SECONDS_PER_METER,
};

use crate::raptor::timetable::{Route, RouteStop, Stop, Time, Timetable, Trip};

pub struct Router<'a, T: Timetable<'a>> {
    timetable: T,
    transfer_graph: Arc<TransferGraph<FastGraphStatic<'a>, SphereIndexMmap<'a, usize>>>,
}

impl<'a, T: Timetable<'a>> Router<'a, T> {
    pub fn new(timetable: T, transfer_graph_path: PathBuf) -> Result<Router<'a, T>, anyhow::Error> {
        info!("Opening transfer graph metadata db.");
        let database = Arc::new(redb::Database::open(
            transfer_graph_path.join("graph_metadata.db"),
        )?);
        info!("Opening transfer graph.");
        let transfer_graph = Arc::new(
            TransferGraph::<FastGraphStatic, SphereIndexMmap<usize>>::read_from_dir(
                transfer_graph_path.clone(),
                database,
            )?,
        );
        info!("Built router");
        Ok(Router {
            timetable,
            transfer_graph,
        })
    }

    pub fn nearest_stops(
        &'a self,
        location: LatLng,
        max_stops: Option<usize>,
        max_distance: Option<f64>,
    ) -> Vec<&'a Stop> {
        let mut stops: Vec<&'a Stop> = vec![];
        assert!(max_stops.is_some() || max_distance.is_some());
        for (count, (stop, dist_sq)) in self
            .timetable
            .nearest_stops(location.lat.deg(), location.lng.deg(), 100)
            .iter()
            .enumerate()
        {
            if let Some(max_stops) = max_stops {
                if count >= max_stops {
                    break;
                }
            }
            if let Some(max_distance) = max_distance {
                if *dist_sq > max_distance {
                    break;
                }
            }
            stops.push(self.timetable.stop(stop.id()));
        }
        stops
    }

    pub async fn route(
        &'a self,
        route_start_time: Time,
        start_location: LatLng,
        target_location: LatLng,
        max_distance_meters: Option<f64>,
        max_candidate_stops_each_side: Option<usize>,
        max_transfers: Option<usize>,
        max_transfer_delta: Option<usize>,
    ) -> SolariResponse {
        let start_stops = self.nearest_stops(
            start_location,
            max_candidate_stops_each_side,
            max_distance_meters,
        );
        let target_stops = self.nearest_stops(
            target_location,
            max_candidate_stops_each_side,
            max_distance_meters,
        );

        let target_costs: Vec<(usize, u32)> = target_stops
            .iter()
            .map(|stop| {
                (
                    stop.id(),
                    (FAKE_WALK_SPEED_SECONDS_PER_METER
                        * stop.location().distance(&target_location).rad()
                        * EARTH_RADIUS_APPROX) as u32,
                )
            })
            .collect();

        let mut context = RouterContext {
            best_times_global: vec![None; self.timetable.stop_count()],
            best_times_per_round: Vec::new(),
            marked_stops: vec![false; self.timetable.stop_count()],
            marked_routes: RefCell::new(vec![
                TripStopTime::marked();
                self.timetable.routes().len()
            ]),
            timetable: &self.timetable,
            round: 0,
            targets: target_costs.clone(),
            max_transfers,
            max_transfer_delta,
            step_log: vec![InternalStep {
                previous_step: 0usize,
                from: InternalStepLocation::Location(LatLng::from_degrees(0.0, 0.0)),
                to: InternalStepLocation::Location(LatLng::from_degrees(0.0, 0.0)),
                route: None,
                departure: Time::epoch(),
                arrival: Time::epoch(),
                trip: None,
            }],
        };
        context
            .init(route_start_time, start_location, &start_stops)
            .await;
        context.route().await;

        let best_itineraries = self
            .pick_best_itineraries(&context, &target_costs)
            .iter()
            .map(|itinerary| {
                self.unwind_itinerary(
                    &context,
                    itinerary,
                    route_start_time,
                    &target_costs,
                    start_location,
                    target_location,
                )
            })
            .collect();

        SolariResponse {
            status: ResponseStatus::Ok,
            itineraries: best_itineraries,
        }
    }

    fn unwind_itinerary(
        &'a self,
        context: &RouterContext<'a, T>,
        itinerary: &InternalItinerary,
        route_start_time: Time,
        target_costs: &[(usize, u32)],
        start_location: LatLng,
        target_location: LatLng,
    ) -> SolariItinerary {
        let mut steps = vec![];
        let mut step_cursor = itinerary.last_step;
        {
            let step = &context.step_log[step_cursor];
            let from = if let InternalStepLocation::Stop(stop) = step.to {
                stop
            } else {
                panic!();
            };
            let to = if let InternalStepLocation::Stop(stop) = step.to {
                stop
            } else {
                panic!();
            };
            let from_location = from.location();
            let last_leg_cost = target_costs
                .iter()
                .find(|(target, _cost)| target == &to.id())
                .map(|(_target, cost)| *cost)
                .expect("Target cost not found");
            steps.push((
                Step::End(EndStep {
                    last_stop: from.metadata(&self.timetable).name.clone(),
                    last_stop_latlng: [from_location.lat.deg(), from_location.lng.deg()],
                    last_stop_departure_epoch_seconds: step.arrival.epoch_seconds() as u64,
                    end_latlng: [target_location.lat.deg(), target_location.lng.deg()],
                    end_epoch_seconds: (step.arrival.epoch_seconds() + last_leg_cost) as u64,
                }),
                step_cursor,
            ));
        }
        while context.step_log[step_cursor].previous_step != 0 {
            let step = &context.step_log[step_cursor];
            let to = if let InternalStepLocation::Stop(stop) = step.to {
                stop
            } else {
                panic!();
            };
            let from = if let InternalStepLocation::Stop(stop) = step.from {
                stop
            } else {
                panic!();
            };
            let to_location = to.location();
            let from_location = from.location();

            steps.push((
                if step.route.is_none() {
                    Step::Transfer(TransferStep {
                        from_stop: from.metadata(&self.timetable).name.clone(),
                        from_stop_latlng: [from_location.lat.deg(), from_location.lng.deg()],
                        to_stop: to.metadata(&self.timetable).name.clone(),
                        to_stop_latlng: [to_location.lat.deg(), to_location.lng.deg()],
                        departure_epoch_seconds: step.departure.epoch_seconds() as u64,
                        arrival_epoch_seconds: step.arrival.epoch_seconds() as u64,
                    })
                } else {
                    let to_location = to.location();
                    let from_location = from.location();

                    let shape = self.clip_shape(step);

                    Step::Trip(TripStep {
                        on_route: step
                            .trip
                            .unwrap()
                            .metadata(&self.timetable)
                            .route_name
                            .clone(),
                        agency: step
                            .trip
                            .unwrap()
                            .metadata(&self.timetable)
                            .agency_name
                            .clone(),
                        departure_stop: from.metadata(&self.timetable).name.clone(),
                        departure_stop_latlng: [from_location.lat.deg(), from_location.lng.deg()],
                        departure_epoch_seconds: step.departure.epoch_seconds() as u64,
                        arrival_stop: to.metadata(&self.timetable).name.clone(),
                        arrival_stop_latlng: [to_location.lat.deg(), to_location.lng.deg()],
                        arrival_epoch_seconds: step.arrival.epoch_seconds() as u64,
                        shape,
                    })
                },
                step_cursor,
            ));
            step_cursor = step.previous_step;
        }
        let end_time = if let Some((Step::End(end), _)) = steps.first() {
            end.end_epoch_seconds
        } else {
            panic!("First step is not a Begin step.");
        };
        let transfer_graph = self.transfer_graph.clone();
        let mut search_context = TransferGraphSearcher::new(transfer_graph);
        let legs = steps
            .iter()
            .rev()
            .filter_map(|(step, _)| match step {
                Step::Trip(trip) => Some(SolariLeg::Transit {
                    start_time: OffsetDateTime::from_unix_timestamp(
                        trip.departure_epoch_seconds as i64,
                    )
                    .expect("Invalid Unix timestamp"),
                    end_time: OffsetDateTime::from_unix_timestamp(
                        trip.arrival_epoch_seconds as i64,
                    )
                    .expect("Invalid Unix timestamp"),
                    start_location: crate::api::LatLng {
                        lat: trip.departure_stop_latlng[0],
                        lon: trip.departure_stop_latlng[1],
                        stop: trip.departure_stop.clone(),
                    },
                    end_location: crate::api::LatLng {
                        lat: trip.arrival_stop_latlng[0],
                        lon: trip.arrival_stop_latlng[1],
                        stop: trip.arrival_stop.clone(),
                    },
                    transit_route: trip.on_route.clone(),
                    transit_agency: trip.agency.clone(),
                    route_shape: trip.shape.clone(),
                }),
                Step::Transfer(transfer) => {
                    let from_coord = Coord {
                        y: transfer.from_stop_latlng[0],
                        x: transfer.from_stop_latlng[1],
                    };
                    let to_coord = Coord {
                        y: transfer.to_stop_latlng[0],
                        x: transfer.to_stop_latlng[1],
                    };
                    let transfer_shape = match self.transfer_graph.transfer_path(
                        &mut search_context,
                        &from_coord,
                        &to_coord,
                    ) {
                        Ok(transfer_path) => Some(transfer_path.shape),
                        Err(err) => {
                            log::error!(
                                "Failed to calculate transfer path: {}, step: {:?}",
                                err,
                                transfer
                            );
                            None
                        }
                    };
                    Some(SolariLeg::Transfer {
                        start_time: OffsetDateTime::from_unix_timestamp(
                            transfer.departure_epoch_seconds as i64,
                        )
                        .expect("Invalid Unix timestamp"),
                        end_time: OffsetDateTime::from_unix_timestamp(
                            transfer.arrival_epoch_seconds as i64,
                        )
                        .expect("Invalid Unix timestamp"),
                        start_location: crate::api::LatLng {
                            lat: transfer.from_stop_latlng[0],
                            lon: transfer.from_stop_latlng[1],
                            stop: transfer.from_stop.clone(),
                        },
                        end_location: crate::api::LatLng {
                            lat: transfer.to_stop_latlng[0],
                            lon: transfer.to_stop_latlng[1],
                            stop: transfer.to_stop.clone(),
                        },
                        route_shape: transfer_shape,
                    })
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        SolariItinerary {
            start_location: crate::api::LatLng {
                lat: start_location.lat.deg(),
                lon: start_location.lng.deg(),
                stop: None,
            },
            end_location: crate::api::LatLng {
                lat: target_location.lat.deg(),
                lon: target_location.lng.deg(),
                stop: None,
            },
            start_time:
                OffsetDateTime::from_unix_timestamp(route_start_time.epoch_seconds() as i64)
                    .expect("Invalid Unix timestamp"),
            end_time: OffsetDateTime::from_unix_timestamp(end_time as i64)
                .expect("Invalid Unix timestamp"),
            legs,
        }
    }

    fn cost_scaling_final_transfer(
        &self,
        context: &RouterContext<'a, T>,
        itinerary: &InternalItinerary,
        scalar: f64,
    ) -> u32 {
        let last_step = &context.step_log[itinerary.last_step];
        if last_step.trip.is_none() {
            let last_step_duration =
                last_step.arrival.epoch_seconds() - last_step.departure.epoch_seconds();
            let scaled = last_step_duration as f64 * scalar;
            return last_step.departure.epoch_seconds() + scaled as u32;
        } else {
            return last_step.arrival.epoch_seconds();
        }
    }

    fn pick_best_itineraries(
        &self,
        context: &RouterContext<'a, T>,
        target_costs: &[(usize, u32)],
    ) -> Vec<InternalItinerary> {
        let mut itineraries = HashSet::new();

        let walking_scalars = [0.5, 1.0, 2.0];
        for round in 0..=context.round {
            for walking_scalar in walking_scalars {
                if let Some((itinerary, _)) = target_costs
                    .iter()
                    .filter_map(|(target_id, cost)| {
                        context.best_times_per_round[round as usize][*target_id]
                            .as_ref()
                            .map(|it| (it, *cost as f64 * walking_scalar))
                    })
                    .min_by_key(|(it, cost)| {
                        self.cost_scaling_final_transfer(context, *it, walking_scalar)
                            + *cost as u32
                    })
                {
                    itineraries.insert(itinerary.clone());
                }
            }
        }

        itineraries.into_iter().collect()
    }

    fn clip_shape(&'a self, step: &InternalStep) -> Option<String> {
        if let Some(route) = &step.route {
            if let Some(shape) = self.timetable.route_shape(route) {
                let departure_stop_distance = if let InternalStepLocation::Stop(stop) = step.from {
                    route
                        .route_stops(&self.timetable)
                        .iter()
                        .filter(|route_stop| route_stop.stop(&self.timetable).id() == stop.id())
                        .next()
                        .map(|route_stop| route_stop.distance_along_route())?
                } else {
                    return None;
                };
                let arrival_stop_distance = if let InternalStepLocation::Stop(stop) = step.to {
                    route
                        .route_stops(&self.timetable)
                        .iter()
                        .filter(|route_stop| route_stop.stop(&self.timetable).id() == stop.id())
                        .next()
                        .map(|route_stop| route_stop.distance_along_route())?
                } else {
                    return None;
                };
                let mut coords = shape
                    .iter()
                    .skip_while(|coord| {
                        coord
                            .distance_along_shape()
                            .map(|dist| dist.is_nan() || dist < departure_stop_distance)
                            .unwrap_or(true)
                    })
                    .take_while(|coord| {
                        coord
                            .distance_along_shape()
                            .map(|dist| dist.is_nan() || dist < arrival_stop_distance)
                            .unwrap_or(true)
                    })
                    .map(|coord| Coord {
                        x: coord.lon(),
                        y: coord.lat(),
                    })
                    .collect::<Vec<_>>();

                if coords.is_empty() {
                    let points: Vec<Coord> = shape
                        .iter()
                        .map(|coord| Coord {
                            x: coord.lon(),
                            y: coord.lat(),
                        })
                        .collect();
                    let start =
                        Point::new(step.from.latlng().lng.deg(), step.from.latlng().lat.deg());
                    let end = Point::new(step.to.latlng().lng.deg(), step.to.latlng().lat.deg());
                    if let (Some((start_idx, start_point)), Some((end_idx, end_point))) = (
                        Self::closest_point(&start, &points),
                        Self::closest_point(&end, &points),
                    ) {
                        coords = shape
                            .iter()
                            .skip(start_idx + 1)
                            .take(end_idx - start_idx)
                            .map(|coord| Coord {
                                x: coord.lon(),
                                y: coord.lat(),
                            })
                            .collect();
                        coords.insert(0, start_point.0);
                        coords.push(end_point.0);
                    }
                }

                let line_string = LineString::new(coords);
                return polyline::encode_coordinates(line_string, 5).ok();
            }
        };
        None
    }

    fn closest_point(target: &Point, points: &Vec<Coord>) -> Option<(usize, Point)> {
        let (idx, closest) = points
            .windows(2)
            .map(|window| Line::new(window[0], window[1]))
            .map(|line| line.closest_point(target))
            .enumerate()
            .reduce(|a, b| {
                let closest = a.1.best_of_two(&b.1, *target);
                if a.1 == closest {
                    a
                } else {
                    b
                }
            })?;
        let closest = match closest {
            geo::Closest::Intersection(point) => point,
            geo::Closest::SinglePoint(point) => point,
            geo::Closest::Indeterminate => return None,
        };
        return Some((idx, closest));
    }
}

#[derive(Debug, Clone)]
struct InternalStep<'a> {
    previous_step: usize,
    from: InternalStepLocation<'a>,
    to: InternalStepLocation<'a>,
    route: Option<Route>,
    departure: Time,
    arrival: Time,
    trip: Option<Trip>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct InternalItinerary {
    last_step: usize,
    final_time: Time,
}

#[derive(Debug, Clone, Serialize)]
pub struct BeginStep {
    pub begin_latlng: [f64; 2],
    pub begin_epoch_seconds: u64,
    pub first_stop: String,
    pub first_stop_latlng: [f64; 2],
    pub first_stop_arrival_epoch_seconds: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TripStep {
    pub on_route: Option<String>,
    pub agency: Option<String>,
    pub departure_stop: Option<String>,
    pub departure_stop_latlng: [f64; 2],
    pub departure_epoch_seconds: u64,
    pub arrival_stop: Option<String>,
    pub arrival_stop_latlng: [f64; 2],
    pub arrival_epoch_seconds: u64,
    pub shape: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransferStep {
    pub from_stop: Option<String>,
    pub from_stop_latlng: [f64; 2],
    pub to_stop: Option<String>,
    pub to_stop_latlng: [f64; 2],
    pub departure_epoch_seconds: u64,
    pub arrival_epoch_seconds: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EndStep {
    pub last_stop: Option<String>,
    pub last_stop_latlng: [f64; 2],
    pub last_stop_departure_epoch_seconds: u64,
    pub end_latlng: [f64; 2],
    pub end_epoch_seconds: u64,
}

#[derive(Debug, Clone, Serialize)]
pub enum Step {
    Begin(BeginStep),
    Trip(TripStep),
    Transfer(TransferStep),
    End(EndStep),
}

pub struct RouterContext<'a, T: Timetable<'a>> {
    best_times_global: Vec<Option<InternalItinerary>>,
    best_times_per_round: Vec<Vec<Option<InternalItinerary>>>,
    marked_stops: Vec<bool>,
    marked_routes: RefCell<Vec<TripStopTime>>,
    timetable: &'a T,
    round: u32,
    targets: Vec<(usize, u32)>,
    max_transfers: Option<usize>,
    max_transfer_delta: Option<usize>,
    step_log: Vec<InternalStep<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InternalStepLocation<'a> {
    Stop(&'a Stop),
    Location(LatLng),
}

impl<'a> InternalStepLocation<'a> {
    pub fn latlng(&'a self) -> LatLng {
        match self {
            InternalStepLocation::Stop(stop) => stop.location(),
            InternalStepLocation::Location(latlng) => latlng.clone(),
        }
    }
}

impl<'a, 'b, T: Timetable<'a>> RouterContext<'a, T>
where
    'b: 'a,
{
    fn best_time_to_target(&self) -> Option<Time> {
        self.targets
            .iter()
            .filter_map(|(id, cost)| {
                self.best_times_global[*id]
                    .as_ref()
                    .map(|best_time| best_time.final_time.plus_seconds(*cost))
            })
            .min()
    }

    fn maybe_update_arrival_time_and_route(
        &mut self,
        round: u32,
        from: &InternalStepLocation<'a>,
        departure_time: Time,
        to: &InternalStepLocation<'a>,
        arrival_time: Time,
        via: Option<Route>,
        on_trip: Option<Trip>,
        previous_step: usize,
    ) -> bool {
        if let InternalStepLocation::Stop(stop) = to {
            let is_better_than_destination_global =
                if let Some(best_time) = self.best_time_to_target() {
                    arrival_time < best_time
                } else {
                    true
                };
            if !is_better_than_destination_global {
                return false;
            }
            let is_best_global = if let Some(previous_best) = &self.best_times_global[stop.id()] {
                &arrival_time < &previous_best.final_time
            } else {
                true
            };
            let round = round as usize;
            if is_best_global {
                let latest_step = InternalStep {
                    from: from.clone(),
                    to: to.clone(),
                    route: via,
                    trip: on_trip,
                    departure: departure_time.clone(),
                    arrival: arrival_time.clone(),
                    previous_step,
                };

                self.best_times_global[stop.id()] = Some(InternalItinerary {
                    final_time: arrival_time.clone(),
                    last_step: self.step_log.len(),
                });
                self.best_times_per_round[round][stop.id()] = Some(InternalItinerary {
                    final_time: arrival_time.clone(),
                    last_step: self.step_log.len(),
                });

                self.marked_stops[stop.id()] = true;
                self.step_log.push(latest_step);

                true
            } else {
                false
            }
        } else {
            false
        }
    }

    async fn init(&mut self, time: Time, start_location: LatLng, starts: &[&'a Stop]) {
        self.best_times_per_round
            .push(vec![None; self.timetable.stop_count()]);

        let start_costs: HashMap<usize, u32> = starts
            .iter()
            .enumerate()
            .map(|(i, start)| {
                (
                    i,
                    (FAKE_WALK_SPEED_SECONDS_PER_METER
                        * start.location().distance(&start_location).rad()
                        * EARTH_RADIUS_APPROX) as u32,
                )
            })
            .collect();
        for (stop_option_index, stop) in starts.iter().enumerate() {
            if let Some(cost) = start_costs.get(&stop_option_index) {
                self.maybe_update_arrival_time_and_route(
                    0u32,
                    &InternalStepLocation::Location(start_location),
                    time.clone(),
                    &InternalStepLocation::Stop(stop),
                    time.clone().plus_seconds(*cost),
                    None,
                    None,
                    0,
                );
            }
        }
    }

    fn earliest_trip_from(&self, route_stop: &RouteStop, not_before: &Time) -> Option<Trip> {
        let trips = route_stop.route(self.timetable).route_trips(self.timetable);
        let position = match trips.binary_search_by_key(not_before, |trip| {
            trip.stop_times(self.timetable)[route_stop.stop_seq()].departure()
        }) {
            Ok(position) => position,
            Err(position) => position,
        };
        if position >= trips.len() {
            None
        } else {
            Some(trips[position])
        }
    }

    async fn do_round(&mut self) -> bool {
        {
            let mut marked_routes = self.marked_routes.borrow_mut();
            for val in &mut (*marked_routes) {
                *val = TripStopTime::marked();
            }
            for (stop_id, stop_marked) in self.marked_stops.iter().enumerate() {
                if !*stop_marked {
                    continue;
                }
                self.explore_routes_for_marked_stop(
                    &mut *marked_routes,
                    self.timetable.stop(stop_id),
                    &self.best_times_global[stop_id].as_ref().unwrap().final_time,
                );
            }
        }
        for stop_marked in &mut self.marked_stops {
            *stop_marked = false;
        }

        let mut marked_stops_count = 0usize;
        let marked_routes = self.marked_routes.clone();
        for (route_id, departure) in marked_routes.borrow_mut().iter().enumerate() {
            if departure.trip_index == usize::MAX {
                continue;
            }
            let route = self.timetable.route(route_id);
            let mut current_trip: Option<(Trip, RouteStop)> = None;
            let mut found_first_stop = false;
            let mut departure_stop_seq = 0usize;

            for route_stop in route.route_stops(self.timetable) {
                if route_stop.id() == departure.route_stop(self.timetable).id() {
                    found_first_stop = true;
                }
                if !found_first_stop {
                    departure_stop_seq += 1;
                    continue;
                }
                if let Some((current_trip, current_trip_start)) = &mut current_trip {
                    // TODO: local pruning, target pruning
                    let departure_trip_stop_time =
                        &current_trip.stop_times(self.timetable)[departure_stop_seq];
                    let previous_step = if let Some(previous_step) = self.best_times_global
                        [departure.route_stop(self.timetable).id()]
                    .as_ref()
                    .map(|step| step.last_step.clone())
                    {
                        previous_step
                    } else {
                        log::error!(
                            "No best time for stop {:?}",
                            departure.route_stop(self.timetable)
                        );
                        continue;
                    };
                    if self.maybe_update_arrival_time_and_route(
                        self.round,
                        &InternalStepLocation::Stop(current_trip_start.stop(self.timetable)),
                        departure_trip_stop_time.departure(),
                        &InternalStepLocation::Stop(route_stop.stop(self.timetable)),
                        current_trip.stop_times(self.timetable)[route_stop.stop_seq()].arrival(),
                        Some(current_trip.route(self.timetable)),
                        Some(current_trip.clone()),
                        previous_step,
                    ) {
                        marked_stops_count += 1;

                        if let Some(trip) = self.earliest_trip_from(
                            departure.route_stop(self.timetable),
                            &self.best_times_global[departure.route_stop(self.timetable).id()]
                                .as_ref()
                                .unwrap()
                                .final_time,
                        ) {
                            if trip.stop_times(self.timetable)[route_stop.stop_seq()].arrival()
                                < self.best_times_global[departure.route_stop(self.timetable).id()]
                                    .as_ref()
                                    .unwrap()
                                    .final_time
                            {
                                *current_trip = trip;
                            }
                        }
                    }
                }

                if current_trip.is_none() {
                    current_trip = self
                        .earliest_trip_from(route_stop, &departure.arrival())
                        .map(|trip| (trip, route_stop.clone()));
                }
            }
        }

        let mut marked_transfers_count = 0usize;
        let mut total_transfers_count = 0usize;
        let marked_stops = self.marked_stops.clone();
        for (stop_id, stop_marked) in marked_stops.iter().enumerate() {
            if !stop_marked {
                continue;
            }
            let stop = self.timetable.stop(stop_id);

            for transfer in self.timetable.transfers_from(stop_id) {
                let transfer_to = transfer.to(self.timetable);
                let last_step = if let Some(last_step) = self.best_times_global[stop.id()]
                    .as_ref()
                    .map(|transfer| transfer.last_step)
                    .clone()
                {
                    last_step
                } else {
                    log::error!("No transfer for stop {:?}", stop);
                    continue;
                };
                // Don't transfer twice in a row.
                if self.step_log[last_step].route.is_none() {
                    continue;
                }
                let best_arrival_at_transfer_start = self.best_times_global[stop.id()]
                    .as_ref()
                    .unwrap()
                    .final_time;
                let arrival_at_transfer_end =
                    best_arrival_at_transfer_start.plus_seconds(transfer.time_seconds());
                total_transfers_count += 1;
                if self.maybe_update_arrival_time_and_route(
                    self.round,
                    &InternalStepLocation::Stop(stop),
                    best_arrival_at_transfer_start,
                    &InternalStepLocation::Stop(transfer_to),
                    arrival_at_transfer_end,
                    None,
                    None,
                    last_step,
                ) {
                    marked_transfers_count += 1;
                }
            }
        }
        debug!("Marked {} new stops", marked_stops_count);
        debug!(
            "Marked {} of {} transfers.",
            marked_transfers_count, total_transfers_count
        );

        self.best_times_per_round
            .push(vec![None; self.timetable.stop_count()]);
        marked_stops_count > 0 || marked_transfers_count > 0
    }

    fn explore_routes_for_marked_stop(
        &self,
        marked_routes: &mut [TripStopTime],
        marked_stop: &Stop,
        not_before: &Time,
    ) {
        for stop_route in marked_stop.stop_routes(self.timetable) {
            let route = stop_route.route(self.timetable);
            if marked_routes[route.id()].trip_index == usize::MAX {
                for trip in route.route_trips(self.timetable) {
                    let trip_stop_time = &trip.stop_times(self.timetable)[stop_route.stop_seq()];
                    if &trip_stop_time.departure() < &not_before {
                        continue;
                    }

                    // We don't actually need to handle the case where the departure hasn't been added because of the u32::MAX step at the beginning of do_round.
                    if trip_stop_time.departure() < marked_routes[route.id()].departure() {
                        marked_routes[route.id()] = *trip_stop_time;
                        // Any trips after this one do not need to be examined.
                        break;
                    }
                }
            } else {
                for trip in route.route_trips(self.timetable)
                    [0..(marked_routes[route.id()].trip_index - route.first_route_trip)]
                    .iter()
                    .rev()
                {
                    let trip_stop_time = &trip.stop_times(self.timetable)[stop_route.stop_seq()];
                    if &trip_stop_time.departure() < &not_before {
                        // We are iterating in reverse, so nothing "after" this (before, temporally) needs to be examined.
                        break;
                    }

                    // We don't actually need to handle the case where the departure hasn't been added because of the u32::MAX step at the beginning of do_round.
                    if trip_stop_time.departure() < marked_routes[route.id()].departure() {
                        marked_routes[route.id()] = *trip_stop_time;
                        // We are iterating in reverse, so we can't break here.
                    }
                }
            }
        }
    }

    pub async fn route(&mut self) {
        self.round = 0;
        let mut marked_stops = true;
        let mut round_bound = self.max_transfers;
        while marked_stops {
            if let Some(round_bound) = round_bound {
                if self.round >= round_bound as u32 {
                    break;
                }
            }
            marked_stops = self.do_round().await;
            // Better way to do this maybe?
            if self.best_time_to_target().is_some() {
                if let Some(delta) = self.max_transfer_delta {
                    if let Some(old_bound) = round_bound {
                        round_bound = Some(old_bound.min(self.round as usize + delta));
                    }
                }
            }
            self.round += 1;
        }
    }
}
