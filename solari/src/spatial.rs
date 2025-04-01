use rstar::{PointDistance, RTreeObject, AABB};
use serde::{Deserialize, Serialize};

pub static FAKE_WALK_SPEED_SECONDS_PER_METER: f64 = 2.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedStop {
    pub coords: [f64; 3],
    pub id: usize,
}

impl RTreeObject for IndexedStop {
    type Envelope = AABB<[f64; 3]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.coords[0], self.coords[1], self.coords[2]])
    }
}

impl PointDistance for IndexedStop {
    fn distance_2(
        &self,
        point: &<Self::Envelope as rstar::Envelope>::Point,
    ) -> <<Self::Envelope as rstar::Envelope>::Point as rstar::Point>::Scalar {
        (self.coords[0] - point[0]).powi(2)
            + (self.coords[1] - point[1]).powi(2)
            + (self.coords[2] - point[2]).powi(2)
    }
}
