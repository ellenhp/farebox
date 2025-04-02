use geo::LineString;
use valhalla_graphtile::{
    GraphId,
    graph_tile::{DirectedEdge, NodeInfo},
};

pub struct EdgeRecord<'a> {
    start_node: GraphId,
    geometry: LineString,
    edge: GraphId,
    directed_edge: &'a DirectedEdge,
}

impl<'a> EdgeRecord<'a> {
    pub fn new(
        start_node: GraphId,
        geometry: LineString,
        edge: GraphId,
        directed_edge: &'a DirectedEdge,
    ) -> EdgeRecord<'a> {
        EdgeRecord {
            start_node,
            geometry,
            edge,
            directed_edge,
        }
    }

    pub fn start_node(&self) -> &GraphId {
        &self.start_node
    }

    pub fn geometry(&self) -> &LineString {
        &self.geometry
    }

    pub fn id(&self) -> GraphId {
        self.edge
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
