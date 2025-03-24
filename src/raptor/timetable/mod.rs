pub mod in_memory;
pub mod mmap;

use std::{time::UNIX_EPOCH, u32};

use bytemuck::{Pod, Zeroable};
use chrono::{DateTime, Days, NaiveDate, NaiveDateTime, NaiveTime};

use chrono_tz::Tz;
use redb::TableDefinition;
use rstar::RTree;
use s2::latlng::LatLng;
use serde::{Deserialize, Serialize};

use super::geomath::IndexedStop;

static DAY_SECONDS: u32 = 86_400;
const STOP_METADATA_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("stop_metadata");
const TRIP_METADATA_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("trip_metadata");

pub trait Timetable<'a> {
    fn route(&'a self, route_id: usize) -> &'a Route;
    fn stop(&'a self, stop_id: usize) -> &'a Stop;
    fn stop_count(&self) -> usize;

    fn stops(&'a self) -> &'a [Stop];
    fn routes(&'a self) -> &'a [Route];
    fn stop_routes(&'a self) -> &'a [StopRoute];
    fn route_stops(&'a self) -> &'a [RouteStop];
    fn route_trips(&'a self) -> &'a [Trip];
    fn trip_stop_times(&'a self) -> &'a [TripStopTime];
    fn transfers(&'a self) -> &'a [Transfer];
    fn transfer_index(&'a self) -> &'a [usize];
    fn transfers_from(&'a self, stop_id: usize) -> &'a [Transfer];
    fn stop_index_copy(&'a self) -> RTree<IndexedStop>;
    fn nearest_stops(&'a self, lat: f64, lng: f64, n: usize) -> Vec<(&'a Stop, f64)>;

    fn stop_metadata(&'a self, stop: &Stop) -> gtfs_structures::Stop;
    fn trip_metadata(&'a self, trip: &Trip) -> TripMetadata;
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Pod, Zeroable, Serialize, Deserialize,
)]
#[repr(C)]
pub struct Stop {
    stop_index: usize,
    s2cell: u64,
    first_stop_route_index: usize,
}

impl<'a> Stop {
    pub fn stop_routes(&self, timetable: &'a dyn Timetable<'a>) -> &'a [StopRoute] {
        let range_end = if self.stop_index == timetable.stops().len() - 1 {
            timetable.stop_routes().len()
        } else {
            timetable.stops()[self.stop_index + 1].first_stop_route_index
        };
        &timetable.stop_routes()[self.first_stop_route_index..range_end]
    }

    #[inline]
    pub fn id(&self) -> usize {
        self.stop_index
    }

    #[inline]
    pub fn location(&self) -> LatLng {
        s2::cellid::CellID(self.s2cell).into()
    }

    pub fn metadata(&self, timetable: &'a dyn Timetable<'a>) -> gtfs_structures::Stop {
        timetable.stop_metadata(self).clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct RouteStop {
    route_index: usize,
    stop_index: usize,
    stop_seq: usize,
}

impl<'a> RouteStop {
    #[inline]
    pub fn route(&self, timetable: &'a dyn Timetable<'a>) -> &'a Route {
        &timetable.routes()[self.route_index]
    }

    #[inline]
    pub fn stop_seq(&self) -> usize {
        self.stop_seq
    }

    #[inline]
    pub fn id(&self) -> usize {
        self.stop_index
    }

    #[inline]
    pub fn stop(&self, timetable: &'a dyn Timetable<'a>) -> &'a Stop {
        &timetable.stops()[self.stop_index]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct StopId(pub usize);

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Pod, Zeroable, Serialize, Deserialize,
)]
#[repr(C)]
pub struct Trip {
    trip_index: usize,
    route_index: usize,
    first_trip_stop_time: usize,
    last_trip_stop_time: usize,
}

impl<'a> Trip {
    pub fn stop_times(&self, timetable: &'a dyn Timetable<'a>) -> &'a [TripStopTime] {
        &timetable.trip_stop_times()[self.first_trip_stop_time..self.last_trip_stop_time]
    }

    #[inline]
    pub fn route(&self, timetable: &'a dyn Timetable<'a>) -> Route {
        timetable.routes()[self.route_index].clone()
    }

    pub fn metadata(&self, timetable: &'a dyn Timetable<'a>) -> TripMetadata {
        timetable.trip_metadata(self).clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TripMetadata {
    pub headsign: Option<String>,
    pub route_name: Option<String>,
    pub agency_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct Route {
    route_index: usize,
    first_route_stop: usize,
    pub(crate) first_route_trip: usize,
}

impl<'a> Route {
    pub fn route_stops(&self, timetable: &'a dyn Timetable<'a>) -> &'a [RouteStop] {
        let range_end = if self.route_index == timetable.routes().len() - 1 {
            timetable.route_stops().len()
        } else {
            timetable.routes()[self.route_index + 1].first_route_stop
        };
        &timetable.route_stops()[self.first_route_stop..range_end]
    }

    pub fn route_trips(&self, timetable: &'a dyn Timetable<'a>) -> &'a [Trip] {
        let range_end = if self.route_index == timetable.routes().len() - 1 {
            timetable.route_trips().len()
        } else {
            timetable.routes()[self.route_index + 1].first_route_trip
        };
        &timetable.route_trips()[self.first_route_trip..range_end]
    }

    #[inline]
    pub fn id(&self) -> usize {
        self.route_index
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct StopRoute {
    route_index: usize,
    stop_seq: usize,
}

impl<'a> StopRoute {
    #[inline]
    pub fn route(&'a self, timetable: &'a dyn Timetable<'a>) -> &'a Route {
        &timetable.routes()[self.route_index]
    }

    #[inline]
    pub fn route_id(&self) -> usize {
        self.route_index
    }

    #[inline]
    pub fn stop_seq(&self) -> usize {
        self.stop_seq
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct TripStopTime {
    pub(crate) trip_index: usize,
    pub(crate) route_stop_seq: usize,
    arrival_time: u32,
    departure_time: u32,
}

impl<'a> TripStopTime {
    #[inline]
    pub fn arrival(&self) -> Time {
        Time {
            epoch_seconds: self.arrival_time,
        }
    }

    #[inline]
    pub fn departure(&self) -> Time {
        Time {
            epoch_seconds: self.departure_time,
        }
    }

    pub(crate) fn new(
        trip_index: usize,
        route_stop_seq: usize,
        arrival_time: DateTime<Tz>,
        departure_time: DateTime<Tz>,
    ) -> TripStopTime {
        TripStopTime {
            trip_index,
            route_stop_seq,
            arrival_time: arrival_time.timestamp() as u32,
            departure_time: departure_time.timestamp() as u32,
        }
    }

    pub(crate) fn marked() -> TripStopTime {
        TripStopTime {
            trip_index: usize::MAX,
            route_stop_seq: usize::MAX,
            arrival_time: u32::MAX,
            departure_time: u32::MAX,
        }
    }

    #[inline]
    pub fn route_stop(&self, timetable: &'a dyn Timetable<'a>) -> &'a RouteStop {
        let route = &timetable.route_trips()[self.trip_index].route(timetable);
        // dbg!(route);
        // dbg!(self);
        &timetable.route_stops()[route.first_route_stop + self.route_stop_seq]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Pod, Zeroable)]
#[repr(C)]
pub struct Transfer {
    to: usize,
    from: usize,
    time: u64,
}

impl<'a> Transfer {
    pub fn all_transfers(from: &Stop, timetable: &'a dyn Timetable<'a>) -> &'a [Transfer] {
        let from = from.stop_index;
        let range_end = if from == timetable.transfer_index().len() - 1 {
            timetable.transfers().len()
        } else {
            timetable.transfer_index()[from + 1]
        };
        &timetable.transfers()[timetable.transfer_index()[from]..range_end]
    }

    #[inline]
    pub fn to(&self, timetable: &'a dyn Timetable<'a>) -> &'a Stop {
        timetable.stop(self.to)
    }

    #[inline]
    pub fn time_seconds(&self) -> u32 {
        self.time as u32
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct Time {
    epoch_seconds: u32,
}

impl Time {
    pub fn naive_date_time(&self, service_day: NaiveDate) -> NaiveDateTime {
        NaiveDate::and_time(
            &service_day
                .checked_add_days(Days::new((self.epoch_seconds / DAY_SECONDS) as u64))
                .unwrap(),
            NaiveTime::from_num_seconds_from_midnight_opt(self.epoch_seconds % DAY_SECONDS, 0)
                .unwrap(),
        )
    }

    pub fn plus_seconds(&self, seconds: u32) -> Time {
        Time {
            epoch_seconds: self
                .epoch_seconds
                .checked_add(seconds)
                .unwrap_or(self.epoch_seconds),
        }
    }

    pub fn epoch_seconds(&self) -> u32 {
        return self.epoch_seconds;
    }

    pub fn from_epoch_seconds(seconds: u32) -> Time {
        Time {
            epoch_seconds: seconds,
        }
    }

    pub fn epoch() -> Time {
        Time { epoch_seconds: 0 }
    }

    pub fn now() -> Time {
        Time {
            epoch_seconds: UNIX_EPOCH.elapsed().unwrap().as_secs() as u32,
        }
    }
}

#[cfg(test)]
mod test {
    use chrono::{NaiveDate, NaiveTime};

    use super::Time;

    #[test]
    fn time_with_24hr_service_day() {
        let time = Time {
            epoch_seconds: 12 * 60 * 60,
        };
        let date_time = time.naive_date_time(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap());
        assert_eq!(
            date_time,
            NaiveDate::from_ymd_opt(2020, 1, 1)
                .unwrap()
                .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap())
        );
    }
    #[test]
    fn time_with_25hr_service_day() {
        let time = Time {
            epoch_seconds: 25 * 60 * 60,
        };
        let date_time = time.naive_date_time(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap());
        assert_eq!(
            date_time,
            NaiveDate::from_ymd_opt(2020, 1, 2)
                .unwrap()
                .and_time(NaiveTime::from_hms_opt(1, 0, 0).unwrap())
        );
    }
}
