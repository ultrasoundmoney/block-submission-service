//! # Submission Service
//!
//! Keep a cache of recent block submissions, and make them available over an API. This allows the
//! relay to keep track only of the best bid it's received.
//!
//! ## Configuration
//! See operational_constants.rs.

mod block_submission_consumer;
mod health;
mod operational_constants;
mod server;

use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use block_submission_service::{env::ENV_CONFIG, log, BlockSubmissionKey, JsonValue};
use fred::{
    prelude::{ClientLike, RedisClient},
    types::RedisConfig,
};
use futures::try_join;
use lazy_static::lazy_static;
use lru::LruCache;
use tokio::sync::Notify;
use tracing::info;

use crate::{
    block_submission_consumer::run_cache_submissions_thread,
    operational_constants::BLOCK_SUBMISSION_CACHE_SIZE,
};

lazy_static! {
    static ref LRU_SIZE: NonZeroUsize = NonZeroUsize::new(BLOCK_SUBMISSION_CACHE_SIZE).unwrap();
}
type BlockSubmissionMap = LruCache<BlockSubmissionKey, JsonValue>;

#[tokio::main]
async fn main() -> Result<()> {
    log::init();

    info!("starting block submission service");

    // When one of our threads panics, we want to shutdown the entire program. Most threads
    // communicate over channels, and so will naturally shut down as the channels close. However,
    // the server thread does not. We use this notify to shutdown the server thread when any other
    // thread panics.
    let shutdown_notify = Arc::new(Notify::new());

    let block_submissions = Arc::new(Mutex::new(LruCache::new(*LRU_SIZE)));
    // Set up the shared Redis pool.
    let config = RedisConfig::from_url(&ENV_CONFIG.redis_uri)?;
    let redis_client = RedisClient::new(config, None, None);
    redis_client.connect();
    redis_client
        .wait_for_connect()
        .await
        .context("failed to connect to redis")?;

    let redis_health = health::RedisHealth::new(redis_client.clone());
    let redis_consumer_health = health::RedisConsumerHealth::new();

    let cache_submissions_thread = run_cache_submissions_thread(
        block_submissions.clone(),
        redis_client,
        redis_consumer_health.clone(),
        shutdown_notify.clone(),
    );

    let server_thread = server::run_server_thread(
        block_submissions,
        redis_health,
        redis_consumer_health,
        shutdown_notify,
    );

    try_join!(cache_submissions_thread, server_thread)?;

    Ok(())
}
