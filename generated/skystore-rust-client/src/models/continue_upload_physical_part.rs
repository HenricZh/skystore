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
pub struct ContinueUploadPhysicalPart {
    #[serde(rename = "part_number")]
    pub part_number: i32,
    #[serde(rename = "etag")]
    pub etag: String,
}

impl ContinueUploadPhysicalPart {
    pub fn new(part_number: i32, etag: String) -> ContinueUploadPhysicalPart {
        ContinueUploadPhysicalPart {
            part_number,
            etag,
        }
    }
}


