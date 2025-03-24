use clap::Parser;
use farebox::{
    api::{request::FareboxRequest, response::FareboxResponse},
    raptor::timetable::{mmap::MmapTimetable, Time},
    route::Router,
};
use rocket::{serde::json::Json, State};
use s2::latlng::LatLng;

#[macro_use]
extern crate rocket;

#[post("/v1/plan", data = "<request>")]
async fn plan(
    request: Json<FareboxRequest>,
    router: &State<Router<'_, MmapTimetable<'_>>>,
) -> Json<FareboxResponse> {
    let from = LatLng::from_degrees(request.0.from.lat, request.0.from.lon);
    let to = LatLng::from_degrees(request.0.to.lat, request.0.to.lon);

    let max_transfers = usize::min(10, request.0.max_transfers.0);

    return Json(
        router
            .route(
                Time::from_epoch_seconds(request.0.start_at.unix_timestamp() as u32),
                from,
                to,
                Some(5000f64),
                Some(20),
                Some(max_transfers),
                Some(2),
            )
            .await,
    );
}

#[derive(Parser)]
struct ServeArgs {
    #[arg(short, long)]
    base_path: String,
    #[arg(short, long)]
    valhalla_endpoint: Option<String>,
    #[arg(short, long)]
    port: Option<u16>,
}

#[launch]
fn rocket() -> _ {
    env_logger::init();
    let args = ServeArgs::parse();
    let router = Router::new(
        MmapTimetable::open(&args.base_path.into()).unwrap(),
        args.valhalla_endpoint,
    );

    rocket::build()
        .manage(router)
        .configure(rocket::Config::figment().merge(("port", args.port.unwrap_or(8000))))
        .mount("/", routes![plan])
}
