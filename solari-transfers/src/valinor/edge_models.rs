use geo::LineString;
use valhalla_graphtile::{
    GraphId,
    graph_tile::{DirectedEdge, NodeInfo},
};

pub struct EdgeRecord<'a> {
    geometry: LineString,
    directed_edge: &'a DirectedEdge,
}

impl<'a> EdgeRecord<'a> {
    pub fn new(geometry: LineString, directed_edge: &'a DirectedEdge) -> EdgeRecord<'a> {
        EdgeRecord {
            geometry,
            directed_edge,
        }
    }

    pub fn geometry(&self) -> &LineString {
        &self.geometry
    }

    pub fn directed_edge(&'a self) -> &'a DirectedEdge {
        self.directed_edge
    }
}
pub struct NodeRecord<'a> {
    id: GraphId,
    node_info: &'a NodeInfo,
}

impl<'a> NodeRecord<'a> {
    pub fn new(id: GraphId, node_info: &'a NodeInfo) -> NodeRecord<'a> {
        NodeRecord { id, node_info }
    }

    pub fn id(&'a self) -> &'a GraphId {
        &self.id
    }

    pub fn node_info(&'a self) -> &'a NodeInfo {
        self.node_info
    }
}
