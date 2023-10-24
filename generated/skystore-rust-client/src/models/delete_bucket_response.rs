/*
 * FastAPI
 *
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: 0.1.0
 * 
 * Generated by: https://openapi-generator.tech
 */




#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeleteBucketResponse {
    #[serde(rename = "locators")]
    pub locators: Vec<crate::models::LocateBucketResponse>,
}

impl DeleteBucketResponse {
    pub fn new(locators: Vec<crate::models::LocateBucketResponse>) -> DeleteBucketResponse {
        DeleteBucketResponse {
            locators,
        }
    }
}


