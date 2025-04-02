mod export_edges;

use std::{collections::HashMap, path::PathBuf, sync::RwLock};

use anyhow::bail;
use export_edges::edge_export::enumerate_edges;
use fast_paths::{FastGraph, FastGraphBuilder, InputGraph, PathCalculator, create_calculator};
use geo::{Coord, Geodesic, Length};
use log::info;
use solari_spatial::{IndexedPoint, SphereIndex};
use valhalla_graphtile::{Access, GraphId};

pub struct TransferGraph {
    node_index: SphereIndex<usize>,
    graph: FastGraph,
    calculator: PathCalculator,
}

impl TransferGraph {
    pub fn new(valhalla_tile_dir: &PathBuf) -> Result<TransferGraph, anyhow::Error> {
        let mut geometry = RwLock::new(Vec::new());
        let mut node_map = RwLock::new(HashMap::<GraphId, usize>::new());
        let mut next_node = RwLock::new(0usize);
        let mut graph = RwLock::new(InputGraph::new());
        info!("Enumerating edges in valhalla tiles and constructing input graph.");
        enumerate_edges(valhalla_tile_dir, |node, edges| {
            if !node.node_info().access().contains(Access::Pedestrian) {
                return;
            }
            let start_node_id = Self::ensure_node(
                node.id(),
                node_map.get_mut().unwrap(),
                next_node.get_mut().unwrap(),
            );
            for edge in edges {
                for node in &edge.geometry().0 {
                    geometry
                        .get_mut()
                        .unwrap()
                        .push(IndexedPoint::new(node, start_node_id));
                }
                let end_node_id = Self::ensure_node(
                    &edge.directed_edge().end_node_id(),
                    node_map.get_mut().unwrap(),
                    next_node.get_mut().unwrap(),
                );
                if end_node_id == start_node_id {
                    continue;
                }
                let length_meters = edge.geometry().length::<Geodesic>();
                let weight_mm = length_meters * 1000.0;
                if edge
                    .directed_edge()
                    .forward_access()
                    .contains(Access::Pedestrian)
                {
                    graph.get_mut().unwrap().add_edge(
                        start_node_id,
                        end_node_id,
                        weight_mm as usize,
                    );
                }
                if edge
                    .directed_edge()
                    .reverse_access()
                    .contains(Access::Pedestrian)
                {
                    graph.get_mut().unwrap().add_edge(
                        end_node_id,
                        start_node_id,
                        weight_mm as usize,
                    );
                }
            }
        })?;
        let node_index = SphereIndex::build(geometry.into_inner().expect("Lock failed"));
        let mut graph = graph.into_inner().expect("Lock failed");
        info!("Freezing graph");
        graph.freeze();
        info!("Contracting");
        let graph = FastGraphBuilder::build(&graph);

        Ok(TransferGraph {
            node_index,
            calculator: create_calculator(&graph),
            graph,
        })
    }

    pub fn transfer_distance_mm(&mut self, from: &Coord, to: &Coord) -> Result<u64, anyhow::Error> {
        let from = self.get_nearest_nodes(from);
        let to = self.get_nearest_nodes(to);
        if let Some(path) =
            self.calculator
                .calc_path_multiple_sources_and_targets(&self.graph, from, to)
        {
            // info!("Path: {}", path.get_weight());
            Ok(path.get_weight() as u64)
        } else {
            // info!("No path");
            bail!("No route")
        }
    }

    fn ensure_node(
        node: &GraphId,
        node_map: &mut HashMap<GraphId, usize>,
        next_node_id: &mut usize,
    ) -> usize {
        if let Some(node) = node_map.get(&node) {
            *node
        } else {
            let this_node = *next_node_id;
            node_map.insert(*node, this_node);
            *next_node_id += 1;
            this_node
        }
    }

    fn get_nearest_nodes(&self, coord: &Coord) -> Vec<(usize, usize)> {
        let radius_meters = 50.0;
        let off_road_fudge_factor = 1.1;
        let neighbors = self.node_index.nearest_neighbors(coord, radius_meters);
        neighbors
            .iter()
            .map(|neighbor| {
                (
                    *neighbor.data,
                    (neighbor.approx_distance_meters * 1000.0 * off_road_fudge_factor) as usize,
                )
            })
            .collect()
    }
}
