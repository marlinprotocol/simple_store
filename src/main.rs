use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use std::{env, sync::Arc};
use tokio_postgres::NoTls;
use uuid::Uuid;

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

// Database initialization query
async fn init_db(client: &tokio_postgres::Client) -> Result<(), tokio_postgres::Error> {
    client
        .batch_execute(
            "
        CREATE TABLE IF NOT EXISTS payloads (
            id VARCHAR(36) PRIMARY KEY,
            payload TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL
        );
        DELETE FROM payloads WHERE created_at < NOW() - INTERVAL '4 hours';
    ",
        )
        .await?;
    Ok(())
}

#[post("/store")]
async fn store_payload(
    db_pool: web::Data<Arc<tokio_postgres::Client>>, // Use Arc<Client> for shared ownership
    data: web::Json<PayloadData>,
) -> impl Responder {
    let db_pool = db_pool.clone(); // Clone Arc to maintain reference counting

    let id = Uuid::new_v4();

    let query = "INSERT INTO payloads (id, payload, created_at) VALUES ($1, $2, NOW())";
    let result = db_pool
        .execute(
            query,
            &[&id.to_string(), &data.payload], // No need for created_at parameter
        )
        .await;

    match result {
        Ok(_) => HttpResponse::Ok().json(StoreResponse { id: id.to_string() }),
        Err(err) => {
            log::error!("{}", err.to_string());
            HttpResponse::InternalServerError().body(err.to_string())
        }
    }
}

#[get("/{id}")]
async fn get_payload(
    db_pool: web::Data<Arc<tokio_postgres::Client>>, // Use Arc<Client> for shared ownership
    id: web::Path<Uuid>,
) -> impl Responder {
    let db_pool = db_pool.clone(); // Clone Arc to maintain reference counting

    let query =
        "SELECT payload FROM payloads WHERE id = $1 AND created_at >= NOW() - INTERVAL '4 hours'";

    let row = db_pool.query_opt(query, &[&id.to_string()]).await;

    match row {
        Ok(Some(row)) => {
            let payload: String = row.get(0);
            HttpResponse::Ok().json(PayloadResponse { payload })
        }
        Ok(None) => HttpResponse::NotFound().body("Not found"),
        Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    env_logger::init();

    // Load environment variables
    dotenv::dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    // Connect to the PostgreSQL database
    let (db_client, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .expect("Failed to connect to database");

    let db_client = Arc::new(db_client);

    // Spawn a task to handle the connection
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Database connection error: {}", e);
        }
    });

    // Initialize the database schema
    init_db(&db_client)
        .await
        .expect("Failed to initialize database");

    // Start the Actix Web server
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(db_client.clone()))
            .service(store_payload)
            .service(get_payload)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
