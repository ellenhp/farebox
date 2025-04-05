pub mod valinor;
pub use fast_paths;
use redb::{Database, ReadableTable, TableDefinition, WriteTransaction};

use std::{
    collections::HashMap,
    fs::File,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, RwLock},
};

use crate::valinor::edge_export::enumerate_edges;
use anyhow::{Ok, bail};
use fast_paths::{
    FastGraph, FastGraphBuilder, FastGraphStatic, FastGraphVec, InputGraph, PathCalculator,
    create_calculator,
};
use geo::{Coord, Geodesic, Length, LineString};
use log::{error, info};
use memmap2::MmapOptions;
use solari_spatial::{IndexedPoint, SphereIndex, SphereIndexMmap, SphereIndexVec};
use valhalla_graphtile::{Access, GraphId};

const EDGE_SHAPE_TABLE: TableDefinition<(u64, u64), &[u8]> =
    TableDefinition::new("valhalla_edge_shapes");
const EDGE_LENGTH_TABLE: TableDefinition<(u64, u64), f64> =
    TableDefinition::new("valhalla_edge_lengths");

pub struct TransferGraph<G: FastGraph, I: SphereIndex<usize>> {
    node_index: I,
    graph: G,
    database: Arc<redb::Database>,
}

impl<G: FastGraph, I: SphereIndex<usize>> TransferGraph<G, I> {
    pub fn new(
        valhalla_tile_dir: &PathBuf,
        database: Arc<Database>,
    ) -> Result<TransferGraph<FastGraphVec, SphereIndexVec<usize>>, anyhow::Error> {
        let mut geometry = RwLock::new(Vec::new());
        let mut node_map = RwLock::new(HashMap::<GraphId, usize>::new());
        let mut next_node = RwLock::new(0usize);
        let mut graph = RwLock::new(InputGraph::new());
        info!("Enumerating edges in valhalla tiles and constructing input graph.");
        let txn = database.begin_write()?;
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
                if let Err(err) = Self::push_edge(
                    &txn,
                    start_node_id as u64,
                    end_node_id as u64,
                    length_meters,
                    edge.geometry(),
                ) {
                    error!("Failed to insert edge into database: {:?}", err);
                };

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
        txn.commit()?;
        let node_index = SphereIndexVec::build(geometry.into_inner().expect("Lock failed"));
        let mut graph = graph.into_inner().expect("Lock failed");
        info!("Freezing graph");
        graph.freeze();
        info!("Contracting");
        let graph = FastGraphBuilder::build(&graph);

        Ok(TransferGraph {
            node_index,
            graph,
            database,
        })
    }

    pub fn save_to_dir(&self, dir: PathBuf) -> Result<(), anyhow::Error> {
        self.graph.save_static(dir.join("transfer_graph.bin"))?;
        self.node_index
            .write_to_file(dir.join("transfer_node_index.bin"))?;
        Ok(())
    }

    pub fn read_from_dir<'a>(
        dir: PathBuf,
        database: Arc<Database>,
    ) -> Result<TransferGraph<FastGraphStatic<'a>, SphereIndexMmap<'a, usize>>, anyhow::Error> {
        let graph_file = File::open(dir.join("transfer_graph.bin"))?;
        let graph_mmap = unsafe { MmapOptions::new().map(&graph_file)? };
        let graph = FastGraphStatic::assemble(Pin::new(graph_mmap))?;

        let index_file = File::open(dir.join("transfer_node_index.bin"))?;
        let index_mmap = unsafe { MmapOptions::new().map(&index_file)? };
        let node_index: SphereIndexMmap<'_, usize> =
            SphereIndexMmap::assemble(Pin::new(index_mmap))?;

        Ok(TransferGraph {
            graph,
            node_index,
            database,
        })
    }

    pub fn transfer_path(
        &self,
        search_context: &mut TransferGraphSearcher<G, I>,
        from: &Coord,
        to: &Coord,
    ) -> Result<TransferPath, anyhow::Error> {
        let from = self.get_nearest_nodes(from);
        let to = self.get_nearest_nodes(to);
        if let Some(path) = search_context
            .calculator
            .calc_path_multiple_sources_and_targets(&self.graph, from, to)
        {
            let txn = self.database.begin_read()?;
            let shapes = txn.open_table(EDGE_SHAPE_TABLE)?;
            let mut path_shape: Vec<Coord<f64>> = Vec::new();
            for pair in path.get_nodes().windows(2) {
                let from = pair[0] as u64;
                let to = pair[1] as u64;
                let shape_bytes = shapes
                    .get(&(from, to))?
                    .ok_or(anyhow::anyhow!("No shape found for edge {}, {}", from, to))?
                    .value()
                    .to_vec();
                let shape_string = String::from_utf8(shape_bytes)?;
                let shape_linestring = polyline::decode_polyline(&shape_string, 5)?;
                path_shape.extend(shape_linestring.0);
            }
            return Ok(TransferPath {
                length_mm: path.get_weight() as u64,
                shape: polyline::encode_coordinates(path_shape, 5)?,
            });
        } else {
            bail!("No route")
        }
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
            return Ok(path.get_weight() as u64);
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

    fn push_edge(
        txn: &WriteTransaction,
        from: u64,
        to: u64,
        length: f64,
        shape: &LineString,
    ) -> Result<bool, anyhow::Error> {
        let key = (from, to);
        let lengths = txn.open_table(EDGE_LENGTH_TABLE)?;
        let should_insert_shape = if let Some(previous_len) = lengths.get(&key)? {
            if length < previous_len.value() {
                true
            } else {
                false
            }
        } else {
            true
        };
        if !should_insert_shape {
            return Ok(false);
        }
        let polyline = polyline::encode_coordinates(shape.0.clone(), 5)?;
        let mut shapes = txn.open_table(EDGE_SHAPE_TABLE)?;
        shapes.insert(&key, polyline.as_bytes())?;
        Ok(true)
    }

    fn get_nearest_nodes(&self, coord: &Coord) -> Vec<(usize, usize)> {
        let radius_meters = 50.0;
        let off_road_fudge_factor = 2.0;
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

pub struct TransferPath {
    pub length_mm: u64,
    pub shape: String,
}

pub struct TransferGraphSearcher<G: FastGraph, I: SphereIndex<usize>> {
    calculator: PathCalculator,
    graph: Arc<TransferGraph<G, I>>,
}

impl<G: FastGraph, I: SphereIndex<usize>> TransferGraphSearcher<G, I> {
    pub fn new(graph: Arc<TransferGraph<G, I>>) -> TransferGraphSearcher<G, I> {
        TransferGraphSearcher {
            calculator: create_calculator(&graph.graph),
            graph,
        }
    }
}

impl<G: FastGraph, I: SphereIndex<usize>> Clone for TransferGraphSearcher<G, I> {
    fn clone(&self) -> Self {
        Self {
            calculator: create_calculator(&self.graph.graph),
            graph: self.graph.clone(),
        }
    }
}
