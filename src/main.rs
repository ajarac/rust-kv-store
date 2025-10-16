use crate::key_value::KeyValue;
use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;

mod key_value;
// --- import your KV (adjust the path to your lib/crate) ---

// or `mod keyvalue; use crate::keyvalue::KeyValue;`

#[derive(Clone)]
struct AppState {
    kv: Arc<KeyValue>,
}

#[tokio::main]
async fn main() {
    // state
    let state = AppState {
        kv: Arc::new(KeyValue::new()),
    };

    // routes
    let app = Router::new()
        .route("/healthz", get(|| async { StatusCode::NO_CONTENT }))
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
    let _prev = state.kv.put(key.as_bytes(), &body);
    Ok(StatusCode::NO_CONTENT)
}

async fn del_kv(
    State(_): State<AppState>,
    Path(_): Path<String>,
) -> Result<StatusCode, StatusCode> {
    Ok(StatusCode::NO_CONTENT)
}
