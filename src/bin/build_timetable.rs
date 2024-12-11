use clap::Parser;
use farebox::raptor::timetable::{in_memory::InMemoryTimetable, mmap::MmapTimetable};

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
    let gtfs = gtfs_structures::Gtfs::new(&args.gtfs_path).unwrap();
    let timetable = InMemoryTimetable::from_gtfs(&[gtfs], args.valhalla_endpoint).await;
    MmapTimetable::from_in_memory(&timetable, &args.base_path.into())
        .expect("Failed to build memory-mapped timetable.");
}
