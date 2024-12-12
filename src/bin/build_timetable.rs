use std::{fs, path::PathBuf};

use clap::Parser;
use farebox::raptor::timetable::{in_memory::InMemoryTimetable, mmap::MmapTimetable};
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
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = BuildArgs::parse();
    if fs::metadata(&args.gtfs_path).unwrap().is_dir() {
        let paths: Vec<PathBuf> = fs::read_dir(&args.gtfs_path)
            .unwrap()
            .map(|p| p.unwrap().path())
            .collect();

        let timetables: Vec<gtfs_structures::Gtfs> = paths
            .par_iter()
            .filter_map(|path| {
                let path = path.to_str().unwrap();
                if path.ends_with(".json") {
                    return None;
                }
                if let Ok(feed) = gtfs_structures::Gtfs::from_path(path) {
                    Some(feed)
                } else {
                    println!("Failed to load feed: {:?}", path);
                    None
                }
            })
            .collect();
        let timetable = InMemoryTimetable::from_gtfs(&timetables, args.valhalla_endpoint).await;

        MmapTimetable::from_in_memory(&timetable, &args.base_path.into())
            .expect("Failed to build memory-mapped timetable.");
    } else {
        let gtfs = gtfs_structures::Gtfs::from_path(&args.gtfs_path).unwrap();

        let timetable = InMemoryTimetable::from_gtfs(&[gtfs], args.valhalla_endpoint).await;
        MmapTimetable::from_in_memory(&timetable, &args.base_path.into())
            .expect("Failed to build memory-mapped timetable.");
    }
}
