use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use std::{env, sync::Arc};
use uuid::Uuid;
use aws_config::Region; // Use this for AWS config regions
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

#[derive(Deserialize)]
struct PayloadData {
    payload: String,
}

#[derive(Serialize)]
struct StoreResponse {
    id: String,
}

#[derive(Serialize)]
struct PayloadResponse {
    payload: String,
}

// Store payload in S3
#[post("/store")]
async fn store_payload(
    s3_client: web::Data<Client>,
    data: web::Json<PayloadData>,
) -> impl Responder {

    let max_size = 10 * 1024 * 1024; // 10 MB
    if data.payload.len() > max_size {
        return HttpResponse::BadRequest().body("Payload exceeds 10 MB size limit");
    }
    let id = Uuid::new_v4();
    let bucket_name = env::var("S3_BUCKET").expect("S3_BUCKET must be set");

    let body = ByteStream::from(data.payload.clone().into_bytes());

    let result = s3_client
        .put_object()
        .bucket(bucket_name)
        .key(id.to_string())
        .body(body)
        .send()
        .await;

    match result {
        Ok(_) => HttpResponse::Ok().json(StoreResponse { id: id.to_string() }),
        Err(err) => {
            log::error!("{}", err.to_string());
            HttpResponse::InternalServerError().body(err.to_string())
        }
    }
}


// Retrieve payload from S3
#[get("/{id}")]
async fn get_payload(
    s3_client: web::Data<Client>,
    id: web::Path<Uuid>,
) -> impl Responder {
    let bucket_name = env::var("S3_BUCKET").expect("S3_BUCKET must be set");

    let result = s3_client
        .get_object()
        .bucket(bucket_name)
        .key(id.to_string())
        .send()
        .await;

    match result {
        Ok(resp) => {
            let body = resp.body.collect().await;
            match body {
                Ok(data) => {
                    let payload = String::from_utf8(data.into_bytes().to_vec())
                        .unwrap_or_else(|_| "Failed to parse payload".to_string());
                    HttpResponse::Ok().json(PayloadResponse { payload })
                }
                Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
            }
        }
        Err(err) => {
            log::error!("{}", err.to_string());
            HttpResponse::NotFound().body("Not found")
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    env_logger::init();

    // Load environment variables
    dotenv::dotenv().ok();
    let aws_region = env::var("AWS_REGION").unwrap_or_else(|_| "ap-south-1".to_string());

    // Create the AWS S3 client
    let config = aws_config::from_env().region(Region::new(aws_region)).load().await;
    let s3_client = Client::new(&config);

    let bucket_name = env::var("S3_BUCKET").expect("S3_BUCKET must be set");

    // Ensure the bucket exists (optional)
    let head_bucket_result = s3_client.head_bucket().bucket(&bucket_name).send().await;
    if head_bucket_result.is_err() {
        panic!("Bucket {} does not exist or is not accessible", bucket_name);
    }

    // Start the Actix Web server
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(s3_client.clone()))
            .service(store_payload)
            .service(get_payload)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}