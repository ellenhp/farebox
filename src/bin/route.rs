use std::time::Instant;

use anyhow::Ok;
use clap::Parser;
use farebox::{
    raptor::timetable::{mmap::MmapTimetable, Time},
    route::Router,
};
use s2::latlng::LatLng;

extern crate farebox;

#[derive(Parser)]
struct BuildArgs {
    #[arg(short, long)]
    base_path: String,
    #[arg(short, long)]
    valhalla_endpoint: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::init();
    let args = BuildArgs::parse();
    let router = Router::new(
        MmapTimetable::new(args.base_path.into())?,
        &args.valhalla_endpoint,
    );
    let start_location = LatLng::from_degrees(47.6237098, -122.3222182);
    let target_location = LatLng::from_degrees(47.6501965, -122.3521259);
    let start_time = Instant::now();
    for _ in 0..1 {
        let itinerary = router
            .route(
                Time::from_hms(12, 0, 0),
                start_location,
                target_location,
                Some(1000f64),
                Some(10),
                Some(4),
                Some(2),
            )
            .await;
        let duration = Instant::now().duration_since(start_time);
        dbg!(itinerary);
        dbg!(duration);
    }
    Ok(())
}
