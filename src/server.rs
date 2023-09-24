use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router, Server,
};
use block_submission_service::{
    env::{self, Env, ENV_CONFIG},
    BlockSubmissionKey,
};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::{error, info};

use crate::{
    health::{self, RedisConsumerHealth, RedisHealth},
    BlockSubmissionMap,
};

#[derive(Clone)]
pub struct AppState {
    block_submissions: Arc<Mutex<BlockSubmissionMap>>,
    pub redis_health: RedisHealth,
    pub redis_consumer_health: RedisConsumerHealth,
}

async fn get_submission_by_key(
    state: State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    match params.get("key") {
        Some(key) => match key.parse::<BlockSubmissionKey>() {
            Ok(key) => {
                let mut block_submissions = state.block_submissions.lock().unwrap();
                let submission = block_submissions.get(&key);
                match submission {
                    Some(submission) => {
                        let str = serde_json::to_string(submission).unwrap();
                        (StatusCode::OK, str)
                    }
                    None => {
                        (
                            StatusCode::NOT_FOUND,
                            format!("submission not found for key: {}", key),
                        )
                    }
                }
            }
            Err(e) => {
                (StatusCode::BAD_REQUEST, format!("query parameter key is invalid, expected: <slot>_<proposer_pubkey>_<block_hash>, error: {}", e))
            }
        },
        None => (
            StatusCode::BAD_REQUEST,
            "query parameter key is required".to_string(),
        ),
    }
}

async fn serve(
    block_submissions: Arc<Mutex<BlockSubmissionMap>>,
    redis_health: RedisHealth,
    redis_consumer_health: RedisConsumerHealth,
    shutdown_notify: Arc<Notify>,
) {
    let state = AppState {
        block_submissions,
        redis_consumer_health,
        redis_health,
    };
    let result = {
        let app = Router::new()
            .route("/livez", get(health::get_livez))
            .route("/submission", get(get_submission_by_key))
            .with_state(state);

        let address = match ENV_CONFIG.env {
            // This avoids macOS firewall popups when developing locally.
            Env::Dev => "127.0.0.1",
            Env::Stag | Env::Prod => "0.0.0.0",
        };

        let port = env::get_env_var("PORT").unwrap_or_else(|| "3004".to_string());

        info!(address, port, "server listening");

        let socket_addr = format!("{address}:{port}").parse().unwrap();

        Server::bind(&socket_addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(async {
                shutdown_notify.notified().await;
            })
            .await
            .context("running server")
    };

    match result {
        Ok(_) => info!("server exited"),
        Err(e) => {
            error!(%e, "server exited with error, shutting down service");
            shutdown_notify.notify_waiters();
        }
    }
}

pub fn run_server_thread(
    block_submissions: Arc<Mutex<BlockSubmissionMap>>,
    redis_health: RedisHealth,
    redis_consumer_health: RedisConsumerHealth,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(serve(
        block_submissions,
        redis_health,
        redis_consumer_health,
        shutdown_notify,
    ))
}
