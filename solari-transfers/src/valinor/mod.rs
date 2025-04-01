mod export_edges;

use std::{path::PathBuf, sync::RwLock};

use export_edges::edge_export::enumerate_edges;
use rstar::{AABB, PointDistance, RTreeObject};
use serde::{Deserialize, Serialize};
use solari_geomath::lat_lng_to_cartesian;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TileNode {
    coords: [f64; 3],
    id: u64,
}

impl RTreeObject for TileNode {
    type Envelope = AABB<[f64; 3]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.coords[0], self.coords[1], self.coords[2]])
    }
}

impl PointDistance for TileNode {
    fn distance_2(
        &self,
        point: &<Self::Envelope as rstar::Envelope>::Point,
    ) -> <<Self::Envelope as rstar::Envelope>::Point as rstar::Point>::Scalar {
        (self.coords[0] - point[0]).powi(2)
            + (self.coords[1] - point[1]).powi(2)
            + (self.coords[2] - point[2]).powi(2)
    }
}

pub struct ValhallaGeometry {
    rtree: rstar::RTree<TileNode>,
}

impl ValhallaGeometry {
    pub fn new(valhalla_tile_dir: PathBuf) -> Result<ValhallaGeometry, anyhow::Error> {
        let mut geometry = RwLock::new(Vec::new());
        enumerate_edges(valhalla_tile_dir, |edge| {
            for node in &edge.geometry().0 {
                geometry.get_mut().unwrap().push(TileNode {
                    coords: lat_lng_to_cartesian(node.y, node.x),
                    id: edge.id().value(),
                })
            }
        })?;
        let rtree = rstar::RTree::bulk_load(geometry.into_inner()?);
        Ok(ValhallaGeometry { rtree })
    }
}
