use crate::valinor::edge_models::EdgeRecord;
use bit_set::BitSet;
use log::warn;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use valhalla_graphtile::graph_tile::DirectedEdge;
use valhalla_graphtile::tile_hierarchy::STANDARD_LEVELS;
use valhalla_graphtile::tile_provider::{
    DirectoryTileProvider, GraphTileProvider, GraphTileProviderError,
};
use valhalla_graphtile::{GraphId, RoadUse};

use super::edge_models::NodeRecord;

fn should_skip_edge(edge: &DirectedEdge) -> bool {
    (edge.is_transit_line()) || edge.is_shortcut() || (edge.edge_use() == RoadUse::Ferry)
}

pub fn enumerate_edges<F: FnMut(NodeRecord, Vec<EdgeRecord>)>(
    tile_path: &PathBuf,
    mut action: F,
) -> anyhow::Result<()> {
    let reader = DirectoryTileProvider::new(tile_path.clone(), NonZeroUsize::new(25).unwrap());

    let mut tile_set = HashMap::new();
    let mut node_count: usize = 0;
    for level in &*STANDARD_LEVELS {
        // For each tile in that level...
        let n_tiles = level.tiling_system.n_rows * level.tiling_system.n_cols;

        for tile_id in 0..n_tiles {
            // Get the index pointer for each tile in the level
            let graph_id = GraphId::try_from_components(level.level, u64::from(tile_id), 0)?;
            match reader.get_tile_containing(&graph_id) {
                Ok(tile) => {
                    let tile_node_count = tile.header.node_count() as usize;
                    tile_set.insert(graph_id, node_count);
                    node_count += tile_node_count;
                }
                Err(GraphTileProviderError::TileDoesNotExist) => {
                    // Ignore; not all tiles will exist for extracts
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    // Drop mutability
    let tile_set = tile_set;

    // An efficient way of tracking whether we've seen an edge before
    // TODO: Does this crate actually work for 64-bit values? I also have some doubts about efficiency.
    let mut processed_nodes = BitSet::with_capacity(node_count);

    for (tile_id, node_index_offset) in &tile_set {
        let node_count = reader.get_tile_containing(&tile_id)?.header.node_count() as usize;

        let tile = reader.get_tile_containing(&tile_id)?;

        for index in 0..node_count {
            if processed_nodes.contains(*node_index_offset + index) {
                continue;
            }

            let node_id = tile_id.with_index(index as u64)?;
            let node = tile.get_node(&node_id)?;

            processed_nodes.insert(*node_index_offset + index);

            let mut edges = Vec::new();
            for outbound_edge_index in 0..node.edge_count() {
                let outbound_edge_index = node.edge_index() + outbound_edge_index as u32;
                let edge_id = if let Ok(id) = tile_id.with_index(outbound_edge_index as u64) {
                    id
                } else {
                    warn!("Edge ID not constructed correctly");
                    continue;
                };
                let edge = if let Ok(edge) = tile.get_directed_edge(&edge_id) {
                    edge
                } else {
                    warn!("Directed edge not found");
                    continue;
                };

                let edge_info = if let Ok(edge_info) = tile.get_edge_info(edge) {
                    edge_info
                } else {
                    warn!("Edge info not found");
                    continue;
                };

                // Skip certain edge types based on the config
                if should_skip_edge(edge) {
                    continue;
                }

                edges.push(EdgeRecord::new(edge_info.shape()?.clone(), edge));
            }
            action(NodeRecord::new(node_id, node), edges);
        }
    }
    Ok(())
}
