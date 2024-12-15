use clap::Parser;
use farebox::{
    raptor::timetable::{mmap::MmapTimetable, Time},
    route::Router,
};
use reqwest::StatusCode;
use rocket::State;
use serde::Deserialize;

#[macro_use]
extern crate rocket;

#[derive(Deserialize, Clone)]
struct LatLng {
    lat: f64,
    lng: f64,
}

#[derive(Deserialize)]
struct AirmailResponse {
    features: Vec<LatLng>,
}

fn parse_lat_lng(text: &str) -> Option<LatLng> {
    let parts: Vec<String> = text.split(",").map(|s| s.to_string()).collect();
    if parts.len() != 2 {
        return None;
    }
    let lat = if let Ok(lat) = parts[0].parse() {
        lat
    } else {
        return None;
    };
    let lng = if let Ok(lng) = parts[1].parse() {
        lng
    } else {
        return None;
    };
    return Some(LatLng { lat, lng });
}

async fn geocode(query: &str) -> Option<LatLng> {
    let params = vec![("q", query)];
    if let Ok(response) = reqwest::get(
        reqwest::Url::parse_with_params("https://api2.airmail.rs/search", &params)
            .expect("Failed to generate URL"),
    )
    .await
    {
        if response.status() != StatusCode::OK {
            return None;
        }
        let response: AirmailResponse =
            serde_json::from_str(&response.text().await.unwrap()).unwrap();
        response.features.first().cloned()
    } else {
        return None;
    }
}

async fn parse_place(text: &str) -> Option<LatLng> {
    if let Some(latlng) = parse_lat_lng(text) {
        return Some(latlng);
    } else if let Some(latlng) = geocode(text).await {
        return Some(latlng);
    }
    return None;
}

#[get("/plan/<from>/<to>")]
async fn index(from: &str, to: &str, router: &State<Router<'_, MmapTimetable<'_>>>) -> String {
    let from = parse_place(from).await.unwrap();
    let to = parse_place(to).await.unwrap();
    let from = s2::latlng::LatLng::from_degrees(from.lat, from.lng);
    let to = s2::latlng::LatLng::from_degrees(to.lat, to.lng);

    if let Some(route) = router
        .route(
            Time::from_epoch_seconds(1734242829),
            from,
            to,
            Some(5000f64),
            Some(20),
            Some(10),
            Some(2),
        )
        .await
    {
        return serde_json::to_string_pretty(&route).unwrap();
    };

    return "[]".to_string();
}

#[derive(Parser)]
struct ServeArgs {
    #[arg(short, long)]
    base_path: String,
    #[arg(short, long)]
    valhalla_endpoint: Option<String>,
}

#[launch]
fn rocket() -> _ {
    env_logger::init();
    let args = ServeArgs::parse();
    let router = Router::new(
        MmapTimetable::new(args.base_path.into()).unwrap(),
        args.valhalla_endpoint,
    );

    rocket::build().manage(router).mount("/", routes![index])
}
