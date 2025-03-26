use std::{
    fs,
    hash::{DefaultHasher, Hasher},
    path::PathBuf,
};

use anyhow::bail;
use clap::Parser;
use farebox::raptor::timetable::{in_memory::InMemoryTimetableBuilder, mmap::MmapTimetable};
use gtfs_structures::GtfsReader;
use log::debug;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

extern crate farebox;

#[derive(Parser)]
struct BuildArgs {
    #[arg(short, long)]
    base_path: String,
    #[arg(short, long)]
    gtfs_path: String,
    #[arg(short, long)]
    valhalla_endpoint: Option<String>,
    #[arg(short, long, default_value_t = 1)]
    num_threads: usize,
}

fn process_gtfs<'a>(
    path: &PathBuf,
    base_path: &PathBuf,
) -> Result<MmapTimetable<'a>, anyhow::Error> {
    let feed = if let Ok(feed) = GtfsReader::default().read_from_path(path.to_str().unwrap()) {
        feed
    } else {
        bail!(format!("Failed to load feed: {:?}", path));
    };
    debug!("Processing feed: {:?}", path);
    let mut in_memory_timetable_builder = InMemoryTimetableBuilder::new();
    in_memory_timetable_builder.preprocess_gtfs(&feed).unwrap();
    let hash = {
        let mut hasher = DefaultHasher::new();
        hasher.write(path.to_str().unwrap().as_bytes());
        format!("{:x}", hasher.finish())
    };

    let timetable_dir = base_path.join(hash);
    fs::create_dir_all(&timetable_dir).unwrap();
    Ok(MmapTimetable::from_in_memory(
        &in_memory_timetable_builder,
        &timetable_dir,
    )?)
}

async fn timetable_from_feeds<'a>(
    paths: &[PathBuf],
    base_path: &PathBuf,
    valhalla_endpoint: Option<String>,
) -> Result<MmapTimetable<'a>, anyhow::Error> {
    let paths = paths.to_vec();

    let timetables: Vec<MmapTimetable<'_>> = paths
        .par_iter()
        .filter_map(|path| process_gtfs(&path, base_path).ok())
        .collect();

    // Combine all timetables into a single one
    let timetable = MmapTimetable::concatenate(&timetables, base_path, valhalla_endpoint).await?;
    Ok(timetable)
}

#[tokio::main(worker_threads = 64)]
async fn main() {
    env_logger::init();
    let args = BuildArgs::parse();
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.num_threads)
        .build_global()
        .unwrap();
    if fs::metadata(&args.gtfs_path).unwrap().is_dir() {
        let paths: Vec<PathBuf> = fs::read_dir(&args.gtfs_path)
            .unwrap()
            .map(|p| p.unwrap().path())
            .collect();

        let _timetable =
            timetable_from_feeds(&paths, &args.base_path.into(), args.valhalla_endpoint)
                .await
                .unwrap();
    } else {
        let _timetable = timetable_from_feeds(
            &[args.gtfs_path.into()],
            &args.base_path.into(),
            args.valhalla_endpoint,
        )
        .await
        .unwrap();
    }
}
