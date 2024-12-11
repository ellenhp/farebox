use std::time::{Duration, Instant, SystemTime};

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
    valhalla_endpoint: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::init();
    let args = BuildArgs::parse();
    let router = Router::new(
        MmapTimetable::new(args.base_path.into())?,
        args.valhalla_endpoint,
    );
    let start_location = LatLng::from_degrees(47.8161226, -122.2970119);
    let target_location = LatLng::from_degrees(45.4941787, -122.8094819);
    // Fremont: 47.6501965, -122.3521259
    // Oly: 47.0242819, -122.8989958
    // Lynnwood: 47.8161226, -122.2970119
    // Everett: 47.9793973, -122.1973826
    // Portland: 45.5181987, -122.6201049
    // Beaverton: 45.4941787, -122.8094819
    let start_time = Instant::now();
    for _ in 0..1 {
        let itinerary = router
            .route(
                Time::from_epoch_seconds(
                    SystemTime::UNIX_EPOCH
                        .elapsed()
                        .unwrap()
                        .saturating_sub(Duration::from_secs(3600 * 72))
                        .as_secs() as u32,
                ),
                start_location,
                target_location,
                Some(5000f64),
                Some(10),
                Some(10),
                Some(2),
            )
            .await;
        let duration = Instant::now().duration_since(start_time);
        dbg!(itinerary);
        dbg!(duration);
    }
    Ok(())
}
