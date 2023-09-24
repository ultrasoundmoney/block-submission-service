use std::sync::{Arc, Mutex};

use anyhow::Result;
use block_submission_service::STREAM_NAME;
use fred::prelude::{RedisClient, StreamsInterface};
use futures::{select, FutureExt};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::{debug, error, info, trace};

use crate::{health::RedisConsumerHealth, BlockSubmissionMap};

use self::decode::XReadBlockSubmissions;

mod decode;

// To avoid busywaiting on the Redis stream, we use Redis' block option to allow Redis to wait up
// to READ_SUBMISSIONS_BLOCK_DURATION milliseconds with responding if no new submissions are
// available.
const READ_SUBMISSIONS_BLOCK_MS: u64 = 1000;

// Max number of new submissions to pull at a time.
const SUBMISSIONS_BATCH_SIZE: u64 = 1000;

async fn add_new_submissions_loop(
    block_submissions_map: &Mutex<BlockSubmissionMap>,
    redis_client: &RedisClient,
    redis_consumer_health: &RedisConsumerHealth,
) -> Result<()> {
    let mut last_id_seen: Option<String> = None;

    loop {
        let block_submissions: XReadBlockSubmissions = {
            match last_id_seen.as_ref() {
                Some(last_id_seen) => {
                    redis_client
                        .xread(
                            Some(SUBMISSIONS_BATCH_SIZE),
                            Some(READ_SUBMISSIONS_BLOCK_MS),
                            STREAM_NAME,
                            last_id_seen,
                        )
                        .await
                }
                None => {
                    redis_client
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
                let mut block_submissions = block_submissions_map.lock().expect("lock poisoned");
                for (_key, value) in submissions {
                    block_submissions.put(value.block_submission_key(), value.payload);
                }

                redis_consumer_health.set_last_message_received_now();

                debug!(count = submissions_len, "added new submissions to cache");
            }
        }
    }
}

pub fn run_cache_submissions_thread(
    block_submissions: Arc<Mutex<BlockSubmissionMap>>,
    redis_client: RedisClient,
    redis_consumer_health: RedisConsumerHealth,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn({
        async move {
            select! {
                _ = shutdown_notify.notified().fuse() => {
                    info!("received shutdown signal, shutting down cache submissions thread");
                },
                result = add_new_submissions_loop(&block_submissions, &redis_client, &redis_consumer_health).fuse() => {
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
