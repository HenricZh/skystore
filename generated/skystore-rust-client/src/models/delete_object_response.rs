/*
 * FastAPI
 *
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: 0.1.0
 * 
 * Generated by: https://openapi-generator.tech
 */




#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DeleteObjectResponse {
    #[serde(rename = "locators")]
    pub locators: Vec<crate::models::LocateObjectResponse>,
}

impl DeleteObjectResponse {
    pub fn new(locators: Vec<crate::models::LocateObjectResponse>) -> DeleteObjectResponse {
        DeleteObjectResponse {
            locators,
        }
    }
}

