use anyhow::bail;
use log::{debug, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{result::Result::Ok, time::Duration};
use tokio::time::sleep;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixRequest {
    pub sources: Vec<ValhallaLocation>,
    pub targets: Vec<ValhallaLocation>,
    pub costing: String,
    pub matrix_locations: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValhallaLocation {
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixResponse {
    pub sources_to_targets: Vec<Vec<MatrixLineItem>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixLineItem {
    pub distance: Option<f64>,
    pub time: Option<u32>,
    pub to_index: Option<usize>,
    pub from_index: Option<usize>,
}

async fn matrix_request_inner(client: &Client, url: &str) -> Result<MatrixResponse, anyhow::Error> {
    debug!("Sending matrix request: {}", url);
    let body = client.get(url).send().await?.text().await?;
    return Ok(serde_json::from_str(&body)?);
}

pub async fn matrix_request(
    client: &Client,
    endpoint: &str,
    request: MatrixRequest,
) -> Result<MatrixResponse, anyhow::Error> {
    let url = format!(
        "{}/sources_to_targets?json={}",
        endpoint,
        serde_json::to_string(&request)?
    );
    let retries = 5;
    for retry in 1..=retries {
        if let Ok(response) = matrix_request_inner(client, &url).await {
            return Ok(response);
        } else {
            warn!("Valhalla error. Retries remaining: {}", retries - retry);
            sleep(Duration::from_millis(100)).await;
            continue;
        }
    }
    bail!("Valhalla error")
}
