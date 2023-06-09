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
pub struct LocateObjectResponse {
    #[serde(rename = "id")]
    pub id: i32,
    #[serde(rename = "tag")]
    pub tag: String,
    #[serde(rename = "cloud")]
    pub cloud: String,
    #[serde(rename = "bucket")]
    pub bucket: String,
    #[serde(rename = "region")]
    pub region: String,
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "size", skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(rename = "last_modified", skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
    #[serde(rename = "etag", skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
}

impl LocateObjectResponse {
    pub fn new(id: i32, tag: String, cloud: String, bucket: String, region: String, key: String) -> LocateObjectResponse {
        LocateObjectResponse {
            id,
            tag,
            cloud,
            bucket,
            region,
            key,
            size: None,
            last_modified: None,
            etag: None,
        }
    }
}

