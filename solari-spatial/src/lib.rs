use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    pin::Pin,
    slice,
};

use bytemuck::{Pod, Zeroable, cast_slice};
use geo::Coord;
use log::debug;
use memmap2::Mmap;
use s2::{cell::Cell, cellid::CellID, latlng::LatLng, rect::Rect, region::RegionCoverer, s1::Deg};
use solari_geomath::EARTH_RADIUS_APPROX;

pub struct NearestNeighborResult<'a, D: Sized + Pod + Zeroable> {
    pub approx_distance_meters: f64,
    pub data: &'a D,
}

pub trait SphereIndex<D: Sized + Pod + Zeroable> {
    fn cells(&self) -> &[u64];
    fn data(&self) -> &[D];

    /// Query the tree for the nearest neighbors to a given point.
    fn nearest_neighbors<'a>(
        &'a self,
        coord: &Coord,
        max_radius_meters: f64,
    ) -> Vec<NearestNeighborResult<'a, D>> {
        let region_coverer = RegionCoverer {
            min_level: 18,
            max_level: 30,
            level_mod: 1,
            max_cells: 10,
        };
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
        let mut covering = covering.0;
        covering.sort_unstable_by_key(|cell_id| {
            let cell: Cell = cell_id.into();
            let angle = target_lat_lng.distance(&cell.center().into()).deg();
            let meters = 111000.0 * angle;
            meters as u32
        });
        let mut neighbors = Vec::new();
        for cell_id in &covering {
            let child_begin_index = match self.cells().binary_search(&cell_id.child_begin().0) {
                Ok(found) => found,
                Err(not_found) => not_found.saturating_sub(1),
            };
            let child_end_index = match self.cells().binary_search(&cell_id.child_end().0) {
                Ok(found) => found,
                Err(not_found) => not_found.saturating_sub(1),
            };
            for neighbor_index in child_begin_index..=child_end_index {
                let neighbor_cell: Cell = CellID(self.cells()[neighbor_index]).into();
                let neighbor_lat_lng: LatLng = neighbor_cell.center().into();
                let approx_distance_meters =
                    neighbor_lat_lng.distance(&target_lat_lng).rad() * EARTH_RADIUS_APPROX;
                neighbors.push(NearestNeighborResult {
                    approx_distance_meters,
                    data: &self.data()[neighbor_index],
                });
            }
        }
        neighbors
    }

    fn write_to_file(&self, path: PathBuf) -> Result<(), anyhow::Error> {
        assert_eq!(self.cells().len(), self.data().len());
        let mut file = File::create(path).unwrap();
        let mut writer = BufWriter::new(&mut file);
        writer.write_all(&(self.cells().len() as u64).to_le_bytes())?;
        writer.write_all(cast_slice(self.cells()))?;
        writer.write_all(cast_slice(self.data()))?;
        Ok(())
    }
}

pub struct SphereIndexVec<D: Sized + Pod + Zeroable> {
    cells: Vec<u64>,
    data: Vec<D>,
}

impl<D: Sized + Pod + Zeroable> SphereIndexVec<D> {
    pub fn build(mut points: Vec<IndexedPoint<D>>) -> Self {
        points.sort_unstable_by_key(|point| point.cell);
        Self {
            cells: points.iter().map(|point| point.cell).collect(),
            data: points.into_iter().map(|point| point.data).collect(),
        }
    }
}

impl<D: Sized + Pod + Zeroable> SphereIndex<D> for SphereIndexVec<D> {
    fn cells(&self) -> &[u64] {
        &self.cells
    }

    fn data(&self) -> &[D] {
        &self.data
    }
}

pub struct SphereIndexMmap<'a, D: Sized + Pod + Zeroable> {
    _mmap: Pin<Mmap>,
    // Use associated arrays because bytemuck "Pod" trait doesn't play nice with generics.
    cells: &'a [u64],
    data: &'a [D],
}

impl<'a, D: Sized + Pod + Zeroable> SphereIndex<D> for SphereIndexMmap<'a, D> {
    fn cells(&self) -> &[u64] {
        self.cells
    }

    fn data(&self) -> &[D] {
        self.data
    }
}

impl<'a, D: Sized + Pod + Zeroable> SphereIndexMmap<'a, D> {
    pub fn assemble(mmap: Pin<Mmap>) -> Result<Self, anyhow::Error> {
        debug!("Opening sphere index from mmap of size {}", mmap.len());
        let data = &mmap;
        let size = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
        debug!("Sphere index has length {}", size);
        let data = &data[8..];
        let cells = unsafe {
            let s = cast_slice::<u8, u64>(&data[..(size * 8)]);
            slice::from_raw_parts(s.as_ptr(), s.len())
        };
        let data = &data[(size * 8)..];
        let data = unsafe {
            let s = cast_slice::<u8, D>(&data[..]);
            slice::from_raw_parts(s.as_ptr(), s.len())
        };
        debug!(
            "Built sphere index with {} cells and {} data elements",
            cells.len(),
            data.len()
        );
        assert_eq!(cells.len(), data.len());
        Ok(SphereIndexMmap {
            _mmap: mmap,
            cells,
            data,
        })
    }
}

#[repr(C)]
pub struct IndexedPoint<D: Sized> {
    cell: u64,
    data: D,
}

impl<D: Sized + Pod + Zeroable> IndexedPoint<D> {
    pub fn new(coord: &Coord, data: D) -> IndexedPoint<D> {
        let lat_lng = LatLng::from_degrees(coord.y, coord.x);
        let cell_id: CellID = lat_lng.into();
        IndexedPoint {
            cell: cell_id.0,
            data,
        }
    }
}
