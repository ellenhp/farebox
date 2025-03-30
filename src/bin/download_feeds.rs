use std::{ffi::OsString, path::PathBuf, str::FromStr};

use clap::Parser;
use farebox::dmfr::DistributedMobilityFeedRegistry;
use log::{debug, info};
use tokio::{fs, spawn};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    dmfr_dir: PathBuf,
    #[arg(long)]
    zip_dir: Option<PathBuf>,
}

async fn download_dmfr(path: PathBuf, zip_dir: PathBuf) -> Result<(), anyhow::Error> {
    debug!("Enumerating feeds from {:?}", &path);
    let dmfr: DistributedMobilityFeedRegistry =
        serde_json::from_str(&fs::read_to_string(&path).await?)?;
    for (feed_idx, feed) in dmfr.feeds.iter().enumerate() {
        if let Some(url) = &feed.urls.static_current {
            debug!("Downloading feed from: {:?}", url);
            let response = reqwest::get(url.as_str()).await?;
            let mut filename = path
                .file_name()
                .expect("GTFS feed not a file")
                .to_os_string();
            filename.push(OsString::from_str(&format!(".{feed_idx}.zip"))?);
            let zip_path = zip_dir.join(filename);
            fs::write(&zip_path, response.bytes().await?).await?;
            info!("Wrote zip file to {:?}", zip_path)
        }
    }
    Ok(())
}

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::init();
    let args = Args::parse();

    let mut read = fs::read_dir(&args.dmfr_dir).await?;
    let mut handles = Vec::new();
    while let Some(dmfr) = read.next_entry().await? {
        let path = dmfr.path();
        debug!("Found path {:?}", &path);
        if path
            .extension()
            .map(|str| str.to_string_lossy().to_string())
            != Some("json".to_string())
        {
            debug!("Skipping path {:?}", &path);
            continue;
        }
        let zip_dir = args
            .zip_dir
            .clone()
            .unwrap_or(args.dmfr_dir.clone())
            .clone();
        let handle = spawn(async move { download_dmfr(path, zip_dir) }).await?;
        handles.push(handle);
    }
    for handle in handles {
        match handle.await {
            Ok(_) => {}
            Err(err) => {
                log::warn!("Failed to download a feed: {err}")
            }
        }
    }
    println!("Done");
    Ok(())
}
