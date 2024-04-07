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
pub struct ListPartsRequest {
    #[serde(rename = "bucket")]
    pub bucket: String,
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "upload_id")]
    pub upload_id: String,
    #[serde(rename = "part_number", skip_serializing_if = "Option::is_none")]
    pub part_number: Option<i32>,
}

impl ListPartsRequest {
    pub fn new(bucket: String, key: String, upload_id: String) -> ListPartsRequest {
        ListPartsRequest {
            bucket,
            key,
            upload_id,
            part_number: None,
        }
    }
}
