use serde::{Deserialize, Serialize};

use super::FareboxItinerary;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum ResponseStatus {
    #[serde(rename = "ok")]
    Ok,
    #[serde(rename = "no_route_found")]
    NoRouteFound,
    #[serde(rename = "too_early")]
    TooEarly,
    #[serde(rename = "too_late")]
    TooLate,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FareboxResponse {
    pub status: ResponseStatus,
    pub itineraries: Vec<FareboxItinerary>,
}
