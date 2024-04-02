// Get object without copy-on-read policy
// #[tracing::instrument(level = "info")]
// async fn get_object(
//     &self,
//     req: S3Request<GetObjectInput>,
// ) -> S3Result<S3Response<GetObjectOutput>> {
//     let locator = self
//         .locate_object(req.input.bucket.clone(), req.input.key.clone())
//         .await?;

//     match locator {
//         Some(location) => {
//             self.store_clients
//                 .get(&location.tag)
//                 .unwrap()
//                 .get_object(req.map_input(|mut input: GetObjectInput| {
//                     input.bucket = location.bucket;
//                     input.key = location.key;
//                     input
//                 }))
//                 .await
//         }
//         None => Err(s3s::S3Error::with_message(
//             s3s::S3ErrorCode::NoSuchKey,
//             "Object not found",
//         )),
//     }
// }

#[tracing::instrument(level = "info")]
async fn get_object(
    &self,
    req: S3Request<GetObjectInput>,
) -> S3Result<S3Response<GetObjectOutput>> {
    let start_time = Instant::now();
    let bucket = req.input.bucket.clone();
    let key = req.input.key.clone();
    let vid = req
        .input
        .version_id
        .clone()
        .map(|id| id.parse::<i32>().unwrap());

    let start_cp_time = Instant::now();

    let locator = self.locate_object(bucket.clone(), key.clone(), vid).await?;

    let duration_cp = start_cp_time.elapsed().as_secs_f32();

    let key_clone = key.clone();

    match locator {
        Some(location) => {
            if req.headers.get("X-SKYSTORE-PULL").is_some() {
                if location.tag != self.client_from_region {
                    let get_resp = self
                        .store_clients
                        .get(&location.tag)
                        .unwrap()
                        .get_object(req.map_input(|mut input: GetObjectInput| {
                            input.bucket = location.bucket;
                            input.key = location.key;
                            input.version_id = location.version_id; // physical version
                            input
                        }))
                        .await?;
                    let data = get_resp.output.body.unwrap();

                    let dir_conf_clone = self.dir_conf.clone();
                    let client_from_region_clone = self.client_from_region.clone();
                    let store_clients_clone = self.store_clients.clone();

                    let (mut input_blobs, _) = split_streaming_blob(data, 2); // locators.len() + 1
                    let response_blob = input_blobs.pop();

                    // Spawn a background task to store the object in the local object store
                    tokio::spawn(async move {
                        let start_upload_resp_result = apis::start_upload(
                            &dir_conf_clone,
                            models::StartUploadRequest {
                                bucket: bucket.clone(),
                                key: key.clone(),
                                client_from_region: client_from_region_clone,
                                version_id: vid, // logical version
                                is_multipart: false,
                                copy_src_bucket: None,
                                copy_src_key: None,
                            },
                        )
                        .await;

                        // When version setting is NULL:
                        // In case of multi-concurrent GET request with copy_on_read policy,
                        // only upload if start_upload returns successful, this indicates that the object is not in the local object store
                        // status neither pending nor ready
                        if let Ok(start_upload_resp) = start_upload_resp_result {
                            let locators = start_upload_resp.locators;
                            let request_template = clone_put_object_request(
                                &new_put_object_request(bucket, key),
                                None,
                            );

                            for (locator, input_blob) in
                                locators.into_iter().zip(input_blobs.into_iter())
                            {
                                let client: Arc<Box<dyn ObjectStoreClient>> =
                                    store_clients_clone.get(&locator.tag).unwrap().clone();
                                let req = S3Request::new(clone_put_object_request(
                                    &request_template,
                                    Some(input_blob),
                                ))
                                .map_input(|mut input| {
                                    input.bucket = locator.bucket.clone();
                                    input.key = locator.key.clone();
                                    input
                                });

                                let put_resp = client.put_object(req).await.unwrap();
                                let e_tag = put_resp.output.e_tag.unwrap();
                                let head_resp = client
                                    .head_object(S3Request::new(new_head_object_request(
                                        locator.bucket.clone(),
                                        locator.key.clone(),
                                        put_resp.output.version_id.clone(), // physical version
                                    )))
                                    .await
                                    .unwrap();
                                apis::complete_upload(
                                    &dir_conf_clone,
                                    models::PatchUploadIsCompleted {
                                        id: locator.id,
                                        size: head_resp.output.content_length as u64,
                                        etag: e_tag,
                                        last_modified: timestamp_to_string(
                                            head_resp.output.last_modified.unwrap(),
                                        ),
                                        version_id: head_resp.output.version_id, // physical version
                                    },
                                )
                                .await
                                .unwrap();

                                // let put_latency = start_time.elapsed().as_secs_f32();
                                // let system_time: SystemTime = Utc::now().into();
                                // let timestamp: s3s::dto::Timestamp = system_time.into();

                                // // build the metrics struct
                                // let metrics = OpMetrics {
                                //     timestamp: timestamp_to_string(timestamp),
                                //     latency: put_latency,
                                //     request_region: client_from_region,
                                //     destination_region: locator.tag.clone(),
                                //     key: key,
                                //     size: head_resp.output.content_length as u64,
                                //     op: "PUT".to_string(),
                                // };

                                // // Serialize the instance to a JSON string.
                                // let serialized_metrics = serde_json::to_string(&metrics).unwrap();

                                // let res = write_metrics_to_file(serialized_metrics, "metrics.json");

                                // if let Err(e) = res {
                                //     panic!("Error writing metrics to file: {}", e);
                                // }
                            }
                        }
                    });

                    let response = S3Response::new(GetObjectOutput {
                        body: Some(response_blob.unwrap()),
                        bucket_key_enabled: get_resp.output.bucket_key_enabled,
                        content_length: get_resp.output.content_length,
                        delete_marker: get_resp.output.delete_marker,
                        missing_meta: get_resp.output.missing_meta,
                        parts_count: get_resp.output.parts_count,
                        tag_count: get_resp.output.tag_count,
                        version_id: vid.map(|id| id.to_string()), // logical version
                        ..Default::default()
                    });

                    // let get_latency = start_time.elapsed().as_secs_f32();
                    // let system_time: SystemTime = Utc::now().into();
                    // let timestamp: s3s::dto::Timestamp = system_time.into();

                    // // build the metrics struct
                    // let metrics = OpMetrics {
                    //     timestamp: timestamp_to_string(timestamp),
                    //     latency: get_latency,
                    //     request_region: client_from_region,
                    //     destination_region: location.tag.clone(),
                    //     key: key_clone,
                    //     size: get_resp.output.content_length as u64,
                    //     op: "GET".to_string(),
                    // };

                    // // Serialize the instance to a JSON string.
                    // let serialized_metrics = serde_json::to_string(&metrics).unwrap();

                    // let res = write_metrics_to_file(serialized_metrics, "metrics.json");

                    // if let Err(e) = res {
                    //     panic!("Error writing metrics to file: {}", e);
                    // }

                    return Ok(response);
                } else {
                    let mut res = self
                        .store_clients
                        .get(&self.client_from_region)
                        .unwrap()
                        .get_object(req.map_input(|mut input: GetObjectInput| {
                            input.bucket = location.bucket;
                            input.key = location.key;
                            input.version_id = location.version_id; //logical version
                            input
                        }))
                        .await?;

                    let blob = res.output.body.unwrap();
                    // blob impl Stream
                    // read all bytes from blob
                    let collected_blob = blob
                        .fold(BytesMut::new(), |mut acc, chunk| async move {
                            acc.extend_from_slice(&chunk.unwrap());
                            acc
                        })
                        .await
                        .freeze();
                    let new_body = Some(StreamingBlob::from(Body::from(collected_blob)));
                    res.output.body = new_body;
                    res.output.version_id = vid.map(|id| id.to_string()); // logical version

                    // let get_latency = start_time.elapsed().as_secs_f32();
                    // let system_time: SystemTime = Utc::now().into();
                    // let timestamp: s3s::dto::Timestamp = system_time.into();

                    // // build the metrics struct
                    // let metrics = OpMetrics {
                    //     timestamp: timestamp_to_string(timestamp),
                    //     latency: get_latency,
                    //     request_region: client_from_region,
                    //     destination_region: location.tag.clone(),
                    //     key: key_clone,
                    //     size: res.output.content_length as u64,
                    //     op: "GET".to_string(),
                    // };

                    // // Serialize the instance to a JSON string.
                    // let serialized_metrics = serde_json::to_string(&metrics).unwrap();

                    // let write_res = write_metrics_to_file(serialized_metrics, "metrics.json");

                    // if let Err(e) = write_res {
                    //     panic!("Error writing metrics to file: {}", e);
                    // }

                    return Ok(res);
                }
            } else {
                let mut res = self
                    .store_clients
                    .get(&location.tag)
                    .unwrap()
                    .get_object(req.map_input(|mut input: GetObjectInput| {
                        input.bucket = location.bucket;
                        input.key = location.key;
                        input.version_id = location.version_id; // logical version
                        input
                    }))
                    .await?;
                let blob = res.output.body.unwrap();

                // Assuming 'stream' is your Stream of Bytes
                let collected_blob = blob
                    .fold(BytesMut::new(), |mut acc, chunk| async move {
                        acc.extend_from_slice(&chunk.unwrap());
                        acc
                    })
                    .await
                    .freeze();

                let new_body = Some(StreamingBlob::from(Body::from(collected_blob)));
                res.output.body = new_body;
                res.output.version_id = vid.map(|id| id.to_string()); // logical version

                // let get_latency = start_time.elapsed().as_secs_f32();
                // let system_time: SystemTime = Utc::now().into();
                // let timestamp: s3s::dto::Timestamp = system_time.into();

                // // build the metrics struct
                // let metrics = OpMetrics {
                //     timestamp: timestamp_to_string(timestamp),
                //     latency: get_latency,
                //     request_region: client_from_region,
                //     destination_region: location.tag.clone(),
                //     key: key_clone,
                //     size: res.output.content_length as u64,
                //     op: "GET".to_string(),
                // };

                // // Serialize the instance to a JSON string.
                // let serialized_metrics = serde_json::to_string(&metrics).unwrap();

                // let write_res = write_metrics_to_file(serialized_metrics, "metrics.json");

                // if let Err(e) = write_res {
                //     panic!("Error writing metrics to file: {}", e);
                // }

                let duration = start_time.elapsed().as_secs_f32();

                // write to the local file
                let metrics = SimpleMetrics {
                    latency: duration_cp.to_string() + "," + &duration.to_string(),
                    key: key_clone,
                    size: res.output.content_length as u64,
                    op: "GET".to_string(),
                };

                let _ = write_metrics_to_file(
                    serde_json::to_string(&metrics).unwrap(),
                    "metrics.json",
                );

                return Ok(res);
            }
        }
        None => Err(s3s::S3Error::with_message(
            s3s::S3ErrorCode::NoSuchKey,
            "Object not found",
        )),
    }
}