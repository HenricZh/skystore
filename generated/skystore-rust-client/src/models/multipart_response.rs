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
pub struct MultipartResponse {
    #[serde(rename = "bucket")]
    pub bucket: String,
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "upload_id")]
    pub upload_id: String,
}

impl MultipartResponse {
    pub fn new(bucket: String, key: String, upload_id: String) -> MultipartResponse {
        MultipartResponse {
            bucket,
            key,
            upload_id,
        }
    }
}

