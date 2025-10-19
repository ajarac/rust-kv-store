use crate::bootstrap::rebuild_from_file;
use crate::storage::Storage;
use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use types::key_value::KeyValue;

mod bootstrap;
mod storage;
mod types;

#[derive(Clone)]
struct AppState {
    kv: Arc<KeyValue>,
    storage: Arc<Storage>,
}

#[tokio::main]
async fn main() {
    // state
    let key_value = KeyValue::new();
    let storage = Storage::open("./store").expect("Error opening store storage");

    rebuild_from_file(&key_value, &storage).expect("Error rebuilding storage");

    let state = AppState {
        kv: Arc::new(key_value),
        storage: Arc::new(storage),
    };

    // routes
    let app = Router::new()
        .route("/health", get(|| async { StatusCode::NO_CONTENT }))
        .route("/kv/{key}", get(get_kv).put(put_kv).delete(del_kv))
        .with_state(state);

    // server
    let addr: SocketAddr = "0.0.0.0:8080".parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn get_kv(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Bytes, StatusCode> {
    match state.kv.get(key.as_bytes()) {
        Some(v) => Ok(Bytes::from(v)),
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn put_kv(
    State(state): State<AppState>,
    Path(key): Path<String>,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let key_bytes = key.as_bytes();
    let value = &body;
    match state.storage.append_put(key_bytes, value) {
        Ok(_) => {
            let _prev = state.kv.put(key_bytes, value);
            Ok(StatusCode::OK)
        }
        Err(_) => Err(StatusCode::NOT_FOUND),
    }
}

async fn del_kv(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let key_bytes = key.as_bytes();
    match state.storage.append_delete(key_bytes) {
        Ok(_) => {
            let _prev = state.kv.delete(key.as_bytes());
            Ok(StatusCode::OK)
        }
        Err(_) => Err(StatusCode::NOT_FOUND),
    }
}
