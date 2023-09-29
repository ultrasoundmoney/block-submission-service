//! # Consumer
//!
//! Consumes block submissions received by the relay from a Redis stream and stores them in that
//! same Redis by BlockSubmissionKey.
use std::sync::Arc;

use anyhow::{Context, Result};
use fred::{pool::RedisPool, prelude::StreamsInterface};
use futures::{channel::mpsc::Sender, select, FutureExt, SinkExt};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::{debug, error, info, trace};

use crate::{health::RedisConsumerHealth, BlockSubmission, STREAM_NAME};

use self::decode::XReadBlockSubmissions;

mod decode;

// To avoid busywaiting on the Redis stream, we use Redis' block option to allow Redis to wait up
// to READ_SUBMISSIONS_BLOCK_DURATION milliseconds with responding if no new submissions are
// available.
const READ_SUBMISSIONS_BLOCK_MS: u64 = 1000;

// Max number of new submissions to pull at a time.
const SUBMISSIONS_BATCH_SIZE: u64 = 1000;

async fn add_new_submissions_loop(
    redis_pool: &RedisPool,
    redis_consumer_health: &RedisConsumerHealth,
    mut submissions_tx: Sender<BlockSubmission>,
) -> Result<()> {
    let mut last_id_seen: Option<String> = None;

    loop {
        let block_submissions: XReadBlockSubmissions = {
            match last_id_seen.as_ref() {
                Some(last_id_seen) => redis_pool
                    .xread(
                        Some(SUBMISSIONS_BATCH_SIZE),
                        Some(READ_SUBMISSIONS_BLOCK_MS),
                        STREAM_NAME,
                        last_id_seen,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "failed to read submissions from redis using starting id: {}",
                            last_id_seen
                        )
                    }),
                None => redis_pool
                    .xread(
                        Some(SUBMISSIONS_BATCH_SIZE),
                        Some(READ_SUBMISSIONS_BLOCK_MS),
                        STREAM_NAME,
                        "$",
                    )
                    .await
                    .context("failed to read submissions from redis using starting id: $"),
            }
        }?;

        match block_submissions.0 {
            None => {
                // No new submissions, ask again, using block to avoid busywaiting.
                trace!(
                    "no new submissions, asking again with {}s block",
                    READ_SUBMISSIONS_BLOCK_MS / 1000
                );
            }
            Some(submissions) => {
                // Update the last id seen.
                last_id_seen = submissions.last().map(|(key, _value)| key.clone());

                let submissions_len = submissions.len();

                for (_key, value) in submissions {
                    trace!(?value, "read new submission from redis");

                    if !value.safe_to_propose() {
                        trace!(
                            ?value,
                            "skipping submission because it is not safe to store"
                        );
                        continue;
                    }

                    submissions_tx
                        .feed(value)
                        .await
                        .context("failed to feed a new submission to submissions channel")?;
                }
                submissions_tx
                    .flush()
                    .await
                    .context("failed to flush the submissions channel")?;

                redis_consumer_health.set_last_message_received_now();

                debug!(count = submissions_len, "read new submissions from redis",);
            }
        }
    }
}

pub fn run_consume_submissions_thread(
    redis_consumer_health: RedisConsumerHealth,
    redis_pool: RedisPool,
    shutdown_notify: Arc<Notify>,
    submissions_tx: Sender<BlockSubmission>,
) -> JoinHandle<()> {
    info!("starting cache submissions thread");
    tokio::spawn({
        async move {
            // If another thread (e.g. server thread) would hit an error, we would have no way of
            // knowing and keep running. We use a shutdown_notify channel to signal to shut down.
            select! {
                _ = shutdown_notify.notified().fuse() => {
                    info!("received shutdown signal, shutting down cache submissions thread");
                },
                result = add_new_submissions_loop(&redis_pool, &redis_consumer_health, submissions_tx).fuse() => {
                    match result {
                        Ok(()) => {
                            error!("add new submissions thread exited unexpectedly without error");
                        },
                        Err(e) => {
                            error!(?e, "add new submissions thread hit error, exited");
                            shutdown_notify.notify_waiters();
                        }
                    }
                }
            }
        }
    })
}
