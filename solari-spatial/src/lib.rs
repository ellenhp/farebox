use std::cell::OnceCell;

use geo::Coord;
use rkyv::{Archive, with::Skip};
use s2::{cell::Cell, cellid::CellID, latlng::LatLng, rect::Rect, region::RegionCoverer, s1::Deg};
use solari_geomath::EARTH_RADIUS_APPROX;

pub struct NearestNeighborResult<'a, D: Archive> {
    pub approx_distance_meters: f64,
    pub data: &'a D,
}

#[derive(Archive)]
pub struct SphereIndex<D: Archive> {
    index: Vec<IndexedPoint<D>>,
    #[rkyv(with = Skip)]
    coverage_calculator: OnceCell<RegionCoverer>,
}

impl<D: Archive> SphereIndex<D> {
    /// Query the tree for the nearest neighbors to a given point.
    pub fn nearest_neighbors<'a>(
        &'a self,
        coord: &Coord,
        max_count: usize,
        max_radius_meters: f64,
    ) -> Vec<NearestNeighborResult<'a, D>> {
        let region_coverer = self.coverage_calculator.get_or_init(|| RegionCoverer {
            min_level: 12,
            max_level: 20,
            level_mod: 1,
            max_cells: 20,
        });
        // Prevent division by zero by clamping the cosine calculated later to this minimum value.
        let cos_epsilon = 0.0000001;
        let size = LatLng {
            lat: Deg(max_radius_meters / 111000.0).into(),
            lng: Deg(max_radius_meters
                / 111000.0
                / f64::cos(coord.y.to_radians()).max(cos_epsilon))
            .into(),
        };

        let target_lat_lng = LatLng::from_degrees(coord.y, coord.x);
        let region = Rect::from_center_size(target_lat_lng, size);
        let covering = region_coverer.fast_covering(&region);
        let mut neighbors = Vec::new();
        for cell_id in &covering.0 {
            let child_begin_index = match self
                .index
                .binary_search_by_key(&cell_id.child_begin().0, |point| point.cell)
            {
                Ok(found) => found,
                Err(not_found) => not_found,
            };
            let child_end_index = match self
                .index
                .binary_search_by_key(&cell_id.child_end().0, |point| point.cell)
            {
                Ok(found) => found,
                Err(not_found) => not_found,
            };
            for neighbor_index in child_begin_index..=child_end_index {
                let neighbor_cell: Cell = CellID(self.index[neighbor_index].cell).into();
                let neighbor_lat_lng: LatLng = neighbor_cell.center().into();
                let approx_distance_meters =
                    neighbor_lat_lng.distance(&target_lat_lng).rad() * EARTH_RADIUS_APPROX;
                neighbors.push(NearestNeighborResultInternal {
                    approx_distance_meters,
                    index: neighbor_index,
                });
            }
        }
        neighbors.sort();
        return neighbors
            .iter()
            .take(max_count)
            .map(|result| NearestNeighborResult {
                approx_distance_meters: result.approx_distance_meters,
                data: &self.index[result.index].data,
            })
            .collect::<Vec<_>>();
    }

    pub fn build(mut points: Vec<IndexedPoint<D>>) -> SphereIndex<D> {
        points.sort_unstable_by_key(|point| point.cell);
        SphereIndex {
            index: points,
            coverage_calculator: OnceCell::new(),
        }
    }
}

#[derive(Archive)]
pub struct IndexedPoint<D: Archive> {
    cell: u64,
    data: D,
}

impl<D: Archive> IndexedPoint<D> {
    pub fn new(coord: &Coord, data: D) -> IndexedPoint<D> {
        let lat_lng = LatLng::from_degrees(coord.y, coord.x);
        let cell_id: CellID = lat_lng.into();
        IndexedPoint {
            cell: cell_id.0,
            data,
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd)]
struct NearestNeighborResultInternal {
    approx_distance_meters: f64,
    index: usize,
}

impl Eq for NearestNeighborResultInternal {}

impl Ord for NearestNeighborResultInternal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.approx_distance_meters
            .partial_cmp(&other.approx_distance_meters)
            .unwrap()
    }
}
