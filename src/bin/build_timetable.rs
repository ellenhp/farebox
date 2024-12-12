use std::{fs, path::PathBuf};

use clap::Parser;
use farebox::raptor::timetable::{
    in_memory::{InMemoryTimetable, InMemoryTimetableBuilder},
    mmap::MmapTimetable,
};

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

async fn timetable_from_feeds(
    paths: &[PathBuf],
    valhalla_endpoint: Option<String>,
) -> InMemoryTimetable {
    let in_memory_timetable = InMemoryTimetable::new();
    let timetable = {
        let mut in_memory_timetable_builder =
            InMemoryTimetableBuilder::new(in_memory_timetable, valhalla_endpoint);
        for gtfs in paths.iter().filter_map(|path| {
            if path.ends_with(".json") {
                return None;
            }
            if let Ok(feed) = gtfs_structures::Gtfs::from_path(path.to_str().unwrap()) {
                Some(feed)
            } else {
                println!("Failed to load feed: {:?}", path);
                None
            }
        }) {
            in_memory_timetable_builder
                .preprocess_gtfs(&gtfs)
                .await
                .unwrap();
        }
        in_memory_timetable_builder.calculate_transfers().await;
        in_memory_timetable_builder.to_timetable()
    };
    timetable
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

        let timetable = timetable_from_feeds(&paths, args.valhalla_endpoint).await;
        // let timetable = InMemoryTimetable::from_gtfs(&timetables, args.valhalla_endpoint).await;

        MmapTimetable::from_in_memory(&timetable, &args.base_path.into())
            .expect("Failed to build memory-mapped timetable.");
    } else {
        let gtfs = gtfs_structures::Gtfs::from_path(&args.gtfs_path).unwrap();

        let timetable = InMemoryTimetable::from_gtfs(&[gtfs], args.valhalla_endpoint).await;
        MmapTimetable::from_in_memory(&timetable, &args.base_path.into())
            .expect("Failed to build memory-mapped timetable.");
    }
}
