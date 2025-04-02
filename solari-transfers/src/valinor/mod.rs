mod export_edges;

use std::{collections::HashMap, path::PathBuf, sync::RwLock};

use export_edges::edge_export::enumerate_edges;
use fast_paths::{FastGraph, FastGraphBuilder, InputGraph};
use geo::{Geodesic, Length};
use rkyv::Archive;
use solari_spatial::{IndexedPoint, SphereIndex};
use valhalla_graphtile::{Access, GraphId};

#[derive(Archive)]
pub struct TransferGraph {
    node_index: SphereIndex<u64>,
    graph: FastGraph,
}

impl TransferGraph {
    pub fn new(valhalla_tile_dir: PathBuf) -> Result<TransferGraph, anyhow::Error> {
        let mut geometry = RwLock::new(Vec::new());
        let mut node_map = RwLock::new(HashMap::<GraphId, usize>::new());
        let mut next_node = RwLock::new(0usize);
        let mut graph = RwLock::new(InputGraph::new());
        enumerate_edges(valhalla_tile_dir, |node, edges| {
            if !node.node_info().access().contains(Access::Pedestrian) {
                return;
            }
            let next_node = next_node.get_mut().unwrap();
            node_map.get_mut().unwrap().insert(*node.id(), *next_node);
            *next_node += 1;
            for edge in edges {
                let edge_id = edge.id().value();
                for node in &edge.geometry().0 {
                    geometry
                        .get_mut()
                        .unwrap()
                        .push(IndexedPoint::new(node, edge_id));
                }
                let start_node = node_map.get_mut().unwrap()[edge.start_node()];
                let end_node = node_map.get_mut().unwrap()[&edge.directed_edge().end_node_id()];
                let length_meters = edge.geometry().length::<Geodesic>();
                let weight = length_meters / 1.4 * 1000.0; // Milliseconds.
                graph
                    .get_mut()
                    .unwrap()
                    .add_edge(start_node, end_node, weight as usize);
            }
        })?;
        let node_index = SphereIndex::build(geometry.into_inner().expect("Lock failed"));
        let graph = graph.into_inner().expect("Lock failed");
        let graph = FastGraphBuilder::build(&graph);
        Ok(TransferGraph { node_index, graph })
    }
}
