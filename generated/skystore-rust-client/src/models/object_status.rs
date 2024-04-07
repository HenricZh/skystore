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
pub struct ObjectStatus {
    #[serde(rename = "status")]
    pub status: models::Status,
}

impl ObjectStatus {
    pub fn new(status: models::Status) -> ObjectStatus {
        ObjectStatus { status }
    }
}
