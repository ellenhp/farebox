use geo::LineString;
use valhalla_graphtile::graph_tile::{DirectedEdge, GraphTile};
use valhalla_graphtile::{GraphId, RoadClass};

// TODO: Do we need this?
pub struct EdgePointer<'a> {
    pub graph_id: GraphId,
    pub tile: &'a GraphTile<'a>,
}

impl EdgePointer<'_> {
    pub(crate) fn edge(&self) -> &DirectedEdge {
        self.tile
            .get_directed_edge(&self.graph_id)
            .expect("That wasn't supposed to happen...")
    }
}

pub struct EdgeRecord {
    geometry: LineString,
    road_class: RoadClass,
    edge: GraphId,
}

impl EdgeRecord {
    pub fn new(geometry: LineString, road_class: RoadClass, edge: GraphId) -> EdgeRecord {
        EdgeRecord {
            geometry,
            road_class,
            edge,
        }
    }

    pub fn geometry(&self) -> &LineString {
        &self.geometry
    }

    pub fn id(&self) -> GraphId {
        self.edge
    }
}
