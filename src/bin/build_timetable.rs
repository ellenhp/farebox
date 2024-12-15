use std::{fs, path::PathBuf};

use clap::Parser;
use farebox::raptor::timetable::{in_memory::InMemoryTimetableBuilder, mmap::MmapTimetable};
use futures::future;
use log::debug;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

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

async fn timetable_from_feeds<'a>(
    paths: &[PathBuf],
    base_path: &PathBuf,
    valhalla_endpoint: Option<String>,
) -> Result<MmapTimetable<'a>, anyhow::Error> {
    let paths = paths.to_vec();
    let tasks: Vec<_> = paths
        .into_iter()
        .enumerate()
        .map(|(idx, path)| {
            let base_path = base_path.clone();
            tokio::spawn(async move {
                if path.ends_with(".json") {
                    return None;
                }
                let feed =
                    if let Ok(feed) = gtfs_structures::Gtfs::from_path(path.to_str().unwrap()) {
                        feed
                    } else {
                        println!("Failed to load feed: {:?}", path);
                        return None;
                    };
                debug!("Processing feed: {:?}", path);
                let mut in_memory_timetable_builder = InMemoryTimetableBuilder::new();
                in_memory_timetable_builder
                    .preprocess_gtfs(&feed)
                    .await
                    .unwrap();
                let timetable_dir = base_path.join(idx.to_string());
                fs::create_dir_all(&timetable_dir).unwrap();
                Some(
                    MmapTimetable::from_in_memory(&in_memory_timetable_builder, &timetable_dir)
                        .expect("Failed to create timetable"),
                )
            })
        })
        .collect();
    let timetable_handles: Vec<MmapTimetable> = future::join_all(tasks)
        .await
        .into_iter()
        .filter_map(|r| match r {
            Ok(Some(timetable)) => Some(timetable),
            _ => None,
        })
        .collect();
    // Combine all timetables into a single one
    let timetable =
        MmapTimetable::concatenate(&timetable_handles, base_path, valhalla_endpoint).await?;
    Ok(timetable)
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
