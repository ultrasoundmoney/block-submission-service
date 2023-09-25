//! # Consumer
//!
//! Consumes block submissions received by the relay from a Redis stream and stores them in that
//! same Redis by BlockSubmissionKey.
use std::sync::Arc;

use anyhow::Result;
use block_submission_service::{BlockSubmission, STREAM_NAME};
use fred::{pool::RedisPool, prelude::StreamsInterface};
use futures::{channel::mpsc::Sender, select, FutureExt, SinkExt};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::{debug, error, info, trace};

use crate::health::RedisConsumerHealth;

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
                Some(last_id_seen) => {
                    redis_pool
                        .xread(
                            Some(SUBMISSIONS_BATCH_SIZE),
                            Some(READ_SUBMISSIONS_BLOCK_MS),
                            STREAM_NAME,
                            last_id_seen,
                        )
                        .await
                }
                None => {
                    redis_pool
                        .xread(
                            Some(SUBMISSIONS_BATCH_SIZE),
                            Some(READ_SUBMISSIONS_BLOCK_MS),
                            STREAM_NAME,
                            "$",
                        )
                        .await
                }
            }
        }?;

        match block_submissions.0 {
            None => {
                // No new submissions, ask again, using block to avoid busywaiting.
                trace!("no new submissions, continuing");
            }
            Some(submissions) => {
                // Update the last id seen.
                last_id_seen = submissions.last().map(|(key, _value)| key.clone());

                let submissions_len = submissions.len();

                for (_key, value) in submissions {
                    submissions_tx.send(value).await?;
                }

                redis_consumer_health.set_last_message_received_now();

                debug!(
                    count = submissions_len,
                    "added new submissions to bid submission archive"
                );
            }
        }
    }
}

pub fn run_consume_submissions_thread(
    redis_pool: RedisPool,
    redis_consumer_health: RedisConsumerHealth,
    shutdown_notify: Arc<Notify>,
    submissions_tx: Sender<BlockSubmission>,
) -> JoinHandle<()> {
    tokio::spawn({
        async move {
            select! {
                _ = shutdown_notify.notified().fuse() => {
                    info!("received shutdown signal, shutting down cache submissions thread");
                },
                result = add_new_submissions_loop(&redis_pool, &redis_consumer_health, submissions_tx).fuse() => {
                    match result {
                        Ok(()) => {
                            error!("add new submissions thread stopped unexpectedly");
                        },
                        Err(err) => {
                            error!(?err, "add new submissions thread finished with an error, shutting down");
                            shutdown_notify.notify_waiters();
                        }
                }
                }
            }
        }
    })
}
