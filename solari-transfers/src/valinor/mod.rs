mod export_edges;

use std::{collections::HashMap, fs::File, path::PathBuf, pin::Pin, sync::RwLock};

use anyhow::bail;
use export_edges::edge_export::enumerate_edges;
use fast_paths::{
    FastGraph, FastGraphBuilder, FastGraphStatic, FastGraphVec, InputGraph, PathCalculator,
    create_calculator,
};
use geo::{Coord, Geodesic, Length};
use log::info;
use memmap2::MmapOptions;
use rkyv::{Archive, Deserialize, Serialize};
use solari_spatial::{IndexedPoint, SphereIndex, SphereIndexMmap, SphereIndexVec};
use valhalla_graphtile::{Access, GraphId};

#[derive(Archive, Deserialize, Serialize)]
pub struct TransferGraph<G: FastGraph, I: SphereIndex<usize>> {
    node_index: I,
    graph: G,
}

impl<G: FastGraph, I: SphereIndex<usize>> TransferGraph<G, I> {
    pub fn new(
        valhalla_tile_dir: &PathBuf,
    ) -> Result<TransferGraph<FastGraphVec, SphereIndexVec<usize>>, anyhow::Error> {
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
        let node_index = SphereIndexVec::build(geometry.into_inner().expect("Lock failed"));
        let mut graph = graph.into_inner().expect("Lock failed");
        info!("Freezing graph");
        graph.freeze();
        info!("Contracting");
        let graph = FastGraphBuilder::build(&graph);

        Ok(TransferGraph { node_index, graph })
    }

    pub fn save_to_dir(&self, dir: PathBuf) -> Result<(), anyhow::Error> {
        self.graph.save_static(dir.join("transfer_graph.bin"))?;
        self.node_index
            .write_to_file(dir.join("transfer_node_index.bin"))?;
        Ok(())
    }

    pub fn read_from_dir<'a>(
        dir: PathBuf,
    ) -> Result<TransferGraph<FastGraphStatic<'a>, SphereIndexMmap<'a, usize>>, anyhow::Error> {
        let graph_file = File::open(dir.join("transfer_graph.bin"))?;
        let graph_mmap = unsafe { MmapOptions::new().map(&graph_file)? };
        let graph = FastGraphStatic::assemble(Pin::new(graph_mmap))?;

        let index_file = File::open(dir.join("transfer_node_index.bin"))?;
        let index_mmap = unsafe { MmapOptions::new().map(&index_file)? };
        let node_index: SphereIndexMmap<'_, usize> =
            SphereIndexMmap::assemble(Pin::new(index_mmap))?;

        Ok(TransferGraph { graph, node_index })
    }

    pub fn transfer_distance_mm(
        &self,
        search_context: &mut TransferGraphSearcher<G, I>,
        from: &Coord,
        to: &Coord,
    ) -> Result<u64, anyhow::Error> {
        let from = self.get_nearest_nodes(from);
        let to = self.get_nearest_nodes(to);
        if let Some(path) = search_context
            .calculator
            .calc_path_multiple_sources_and_targets(&self.graph, from, to)
        {
            Ok(path.get_weight() as u64)
        } else {
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

pub struct TransferGraphSearcher<'a, G: FastGraph, I: SphereIndex<usize>> {
    calculator: PathCalculator,
    graph: &'a TransferGraph<G, I>,
}

impl<'a, G: FastGraph, I: SphereIndex<usize>> TransferGraphSearcher<'a, G, I> {
    pub fn new(graph: &'a TransferGraph<G, I>) -> TransferGraphSearcher<'a, G, I> {
        TransferGraphSearcher {
            calculator: create_calculator(&graph.graph),
            graph,
        }
    }
}

impl<'a, G: FastGraph, I: SphereIndex<usize>> Clone for TransferGraphSearcher<'a, G, I> {
    fn clone(&self) -> Self {
        Self {
            calculator: create_calculator(&self.graph.graph),
            graph: self.graph,
        }
    }
}
