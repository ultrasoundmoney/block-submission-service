use std::sync::Arc;

use anyhow::Context;
use axum::{routing::get, Router, Server};
use block_submission_service::env::{self, Env, ENV_CONFIG};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::{error, info};

use crate::health::{self, RedisConsumerHealth, RedisHealth};

#[derive(Clone)]
pub struct AppState {
    pub redis_health: RedisHealth,
    pub redis_consumer_health: RedisConsumerHealth,
}

async fn serve(
    redis_health: RedisHealth,
    redis_consumer_health: RedisConsumerHealth,
    shutdown_notify: Arc<Notify>,
) {
    let state = AppState {
        redis_consumer_health,
        redis_health,
    };
    let result = {
        let app = Router::new()
            .route("/livez", get(health::get_livez))
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
    redis_health: RedisHealth,
    redis_consumer_health: RedisConsumerHealth,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(serve(redis_health, redis_consumer_health, shutdown_notify))
}
