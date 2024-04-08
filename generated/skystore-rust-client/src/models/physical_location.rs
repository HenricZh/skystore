/*
 * FastAPI
 *
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: 0.1.0
 *
 * Generated by: https://openapi-generator.tech
 */

use crate::models;

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct PhysicalLocation {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "cloud")]
    pub cloud: String,
    #[serde(rename = "region")]
    pub region: String,
    #[serde(rename = "bucket")]
    pub bucket: String,
    #[serde(rename = "prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(rename = "is_primary", skip_serializing_if = "Option::is_none")]
    pub is_primary: Option<bool>,
    #[serde(rename = "need_warmup", skip_serializing_if = "Option::is_none")]
    pub need_warmup: Option<bool>,
}

impl PhysicalLocation {
    pub fn new(name: String, cloud: String, region: String, bucket: String) -> PhysicalLocation {
        PhysicalLocation {
            name,
            cloud,
            region,
            bucket,
            prefix: None,
            is_primary: None,
            need_warmup: None,
        }
    }
}
