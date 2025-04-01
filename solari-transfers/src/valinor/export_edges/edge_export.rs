use crate::valinor::export_edges::edge_models::EdgeRecord;
use bit_set::BitSet;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use valhalla_graphtile::graph_tile::{DirectedEdge, LookupError};
use valhalla_graphtile::tile_hierarchy::STANDARD_LEVELS;
use valhalla_graphtile::tile_provider::{
    DirectoryTileProvider, GraphTileProvider, GraphTileProviderError,
};
use valhalla_graphtile::{GraphId, RoadUse};

fn should_skip_edge(edge: &DirectedEdge) -> bool {
    (edge.is_transit_line()) || edge.is_shortcut() || (edge.edge_use() == RoadUse::Ferry)
}

pub fn enumerate_edges<F: FnMut(EdgeRecord)>(
    tile_path: PathBuf,
    mut action: F,
) -> anyhow::Result<()> {
    // TODO: Make this configurable
    let reader = DirectoryTileProvider::new(tile_path, NonZeroUsize::new(25).unwrap());

    // TODO: Almost all code below feels like it can be abstracted into a graph traversal helper...
    // We could even make processing plugins with WASM LOL
    // Enumerate edges in available tiles

    let mut tile_set = HashMap::new();
    let mut edge_count: usize = 0;
    for level in &*STANDARD_LEVELS {
        // For each tile in that level...
        let n_tiles = level.tiling_system.n_rows * level.tiling_system.n_cols;

        for tile_id in 0..n_tiles {
            // Get the index pointer for each tile in the level
            let graph_id = GraphId::try_from_components(level.level, u64::from(tile_id), 0)?;
            match reader.get_tile_containing(&graph_id) {
                Ok(tile) => {
                    let tile_edge_count = tile.header.directed_edge_count() as usize;
                    tile_set.insert(graph_id, edge_count);
                    edge_count += tile_edge_count;
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
    // FIXME: Only works on 64-bit (or higher?) platforms
    // TODO: Does this crate actually work for 64-bit values? I also have some doubts about efficiency.
    // TODO: Should we ever export nodes too in certain cases? Ex: a bollard on an otherwise driveable road?
    let mut processed_edges = BitSet::with_capacity(edge_count);

    for (tile_id, edge_index_offset) in &tile_set {
        let edge_count = reader
            .get_tile_containing(&tile_id)?
            .header
            .directed_edge_count() as usize;

        let tile = reader.get_tile_containing(&tile_id)?;

        for index in 0..edge_count {
            if processed_edges.contains(edge_index_offset + index) {
                continue;
            }

            // TODO: Some TODO about transition edges in the original source

            // Get the edge
            // TODO: Helper for rewriting the index of a graph ID?
            let edge_id = tile_id.with_index(index as u64)?;
            let edge = tile.get_directed_edge(&edge_id)?;

            // TODO: Mark the edge as seen (maybe? Weird TODO in the Valhalla source)
            processed_edges.insert(edge_index_offset + index);

            // Skip certain edge types based on the config
            let edge_info = tile.get_edge_info(edge)?;
            if should_skip_edge(edge) {
                continue;
            }

            // Get the opposing edge

            let opposing_edge = match tile.get_opp_edge_index(&edge_id) {
                Ok(opp_edge_id) => {
                    let opp_graph_id = edge_id.with_index(opp_edge_id as u64)?;
                    opp_graph_id
                }
                Err(LookupError::InvalidIndex) => {
                    return Err(LookupError::InvalidIndex)?;
                }
                Err(LookupError::MismatchedBase) => {
                    let (opp_graph_id, _tile) = reader.get_opposing_edge(&edge_id)?;
                    opp_graph_id
                }
            };
            if let Some(offset) = tile_set.get(&opposing_edge.tile_base_id()) {
                processed_edges.insert(offset + opposing_edge.index() as usize);
            } else {
                // This happens in extracts, but shouldn't for the planet...
                eprintln!("Missing opposite edge {} in tile set", opposing_edge);
            }

            // Keep some state about this section of road
            // let mut edges: Vec<EdgePointer> = vec![EdgePointer {
            //     graph_id: edge_id,
            //     tile: tile.clone(),
            // }];

            // TODO: Traverse forward and backward from the edge as an optimization to coalesce segments with no change?
            // This should be an opt-in behavior for visualization of similar roads,
            // but note that it then no longer becomes 1:1
            // Could also be useful for MLT representation?

            // TODO: Visualize the dead ends? End node in another layer at the end of edges that don't connect?

            // TODO: Coalesce with opposing edge.
            // Seems like we may be able to do something like this:
            //   - Find which edge is "forward"
            //   - Omit forward field
            //   - Check if any difference in edge + opp edge tagging; I'd expect reversed access; anything else? Can test this...
            action(EdgeRecord::new(
                edge_info.shape()?.clone(),
                edge.classification(),
                edge_id,
            ));
        }
    }

    Ok(())
}
