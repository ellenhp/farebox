use serde::{Deserialize, Serialize};

use super::FareboxItinerary;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum ResponseStatus {
    #[serde(alias = "ok")]
    Ok,
    #[serde(alias = "no_route_found")]
    NoRouteFound,
    #[serde(alias = "too_early")]
    TooEarly,
    #[serde(alias = "too_late")]
    TooLate,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FareboxResponse {
    pub status: ResponseStatus,
    pub itineraries: Vec<FareboxItinerary>,
}
