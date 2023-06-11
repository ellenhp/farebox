use anyhow::Ok;
use reqwest::Client;
use serde::{Deserialize, Serialize};

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

pub async fn matrix_request(
    client: &Client,
    endpoint: &str,
    request: MatrixRequest,
) -> Result<MatrixResponse, anyhow::Error> {
    let body = client
        .get(format!(
            "{}/sources_to_targets?json={}",
            endpoint,
            serde_json::to_string(&request)?
        ))
        .send()
        .await?
        .text()
        .await?;
    Ok(serde_json::from_str(&body)?)
}
