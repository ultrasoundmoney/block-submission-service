//! # Submission Service
//!
//! Reads the recently received block submissions from a Redis stream and makes them available
//! under unique keys in that same Redis instance. This allows the relay to keep track only of the
//! best bid it's received.
//!
//! ## Configuration
//! See storage.rs.

use std::sync::Arc;

use anyhow::{Context, Result};
use block_submission_service::{
    env::ENV_CONFIG, log, run_consume_submissions_thread, run_server_thread,
    run_store_submissions_thread, RedisConsumerHealth, RedisHealth,
};
use fred::{pool::RedisPool, types::RedisConfig};
use futures::{channel::mpsc::channel, try_join};
use tokio::sync::Notify;
use tracing::info;

const SUBMISSIONS_BUFFER_SIZE: usize = 64;

#[tokio::main]
async fn main() -> Result<()> {
    log::init();

    info!("starting block submission service");

    // When one of our threads panics, we want to shutdown the entire program. Most threads
    // communicate over channels, and so will naturally shut down as the channels close. However,
    // the server thread does not. We use this notify to shutdown the server thread when any other
    // thread panics.
    let shutdown_notify = Arc::new(Notify::new());

    // Set up the shared Redis pool.
    let config = RedisConfig::from_url(&ENV_CONFIG.redis_uri)?;
    // We use a pool of connections to be able to store submissions in parallel.
    let redis_pool = RedisPool::new(config, None, None, 4)?;
    redis_pool.connect();
    redis_pool
        .wait_for_connect()
        .await
        .context("failed to connect to redis")?;

    let redis_health = RedisHealth::new(redis_pool.clone());
    let redis_consumer_health = RedisConsumerHealth::new();

    let (submissions_tx, submissions_rx) = channel(SUBMISSIONS_BUFFER_SIZE);

    let cache_submissions_thread = run_consume_submissions_thread(
        redis_pool.clone(),
        redis_consumer_health.clone(),
        shutdown_notify.clone(),
        submissions_tx,
    );

    let store_submissions_thread =
        run_store_submissions_thread(redis_pool, submissions_rx, shutdown_notify.clone());

    let server_thread = run_server_thread(redis_health, redis_consumer_health, shutdown_notify);

    try_join!(
        cache_submissions_thread,
        server_thread,
        store_submissions_thread
    )?;

    Ok(())
}
