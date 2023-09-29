use std::sync::Arc;

use anyhow::Result;
use fred::{
    pool::RedisPool,
    prelude::KeysInterface,
    types::{Expiration, RedisValue},
};
use futures::{channel::mpsc::Receiver, StreamExt, TryStreamExt};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::{error, info};

use crate::{performance::BlockCounter, BlockSubmission};

const STORE_MAX_CONCURRENCY: usize = 4;

// How long are the block submissions kept around in Redis. Given bidding starts before a slot, and
// ends about 2 or 3 seconds into a slot anything over 2 slots in range i.e. 24 seconds, should be
// safe. To be extra safe, we double that.
const BLOCK_SUBMISSION_EXPIRATION_SECS: i64 = 48;

async fn store_submissions(
    block_counter: &BlockCounter,
    redis_pool: RedisPool,
    submissions_rx: Receiver<BlockSubmission>,
) -> Result<()> {
    submissions_rx
        .map(Ok)
        .try_for_each_concurrent(STORE_MAX_CONCURRENCY, |block_submission| {
            let redis_pool = redis_pool.clone();
            async move {
                redis_pool
                    .set::<RedisValue, String, RedisValue>(
                        block_submission.block_submission_key().to_string(),
                        RedisValue::String(block_submission.execution_payload().to_string().into()),
                        Some(Expiration::EX(BLOCK_SUBMISSION_EXPIRATION_SECS)),
                        None,
                        false,
                    )
                    .await?;

                block_counter.increment();

                Ok(())
            }
        })
        .await
}

pub fn run_store_submissions_thread(
    block_counter: Arc<BlockCounter>,
    redis_pool: RedisPool,
    shutdown_notify: Arc<Notify>,
    submissions_rx: Receiver<BlockSubmission>,
) -> JoinHandle<()> {
    info!("starting store submissions thread");
    tokio::spawn({
        async move {
            match store_submissions(&block_counter, redis_pool, submissions_rx).await {
                Ok(()) => {
                    info!("store submissions channel closed, store submissions thread exited");
                }
                Err(e) => {
                    error!(?e, "store submissions thread hit error, exited");
                    shutdown_notify.notify_waiters();
                }
            }
        }
    })
}
