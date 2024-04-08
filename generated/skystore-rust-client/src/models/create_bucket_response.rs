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
pub struct CreateBucketResponse {
    #[serde(rename = "locators")]
    pub locators: Vec<models::LocateBucketResponse>,
}

impl CreateBucketResponse {
    pub fn new(locators: Vec<models::LocateBucketResponse>) -> CreateBucketResponse {
        CreateBucketResponse { locators }
    }
}
