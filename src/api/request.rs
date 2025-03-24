use std::usize;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::LatLng;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum TransferMode {
    #[serde(alias = "walking")]
    Walking,
    #[serde(alias = "cycling")]
    Cycling,
}

impl Default for TransferMode {
    fn default() -> Self {
        Self::Walking
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TransferQuantity(pub usize);

impl Default for TransferQuantity {
    fn default() -> Self {
        Self(usize::MAX)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FareboxRequest {
    pub from: LatLng,
    pub to: LatLng,
    #[serde(
        serialize_with = "time::serde::timestamp::milliseconds::serialize",
        deserialize_with = "time::serde::timestamp::milliseconds::deserialize"
    )]
    pub start_at: OffsetDateTime,

    #[serde(default)]
    pub transfer_mode: TransferMode,
    #[serde(default)]
    pub max_transfers: TransferQuantity,
}
