use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use block_submission_service::{
    consumer, env::ENV_CONFIG, health, storage, BlockSubmission, JsonValue, STREAM_NAME,
};
use fred::{
    pool::RedisPool,
    prelude::{KeysInterface, StreamsInterface},
    types::{MultipleOrderedPairs, RedisConfig, RedisValue},
};
use futures::channel::mpsc::channel;
use tokio::{sync::Notify, time::sleep};

#[tokio::test]
async fn store_block_submission() -> Result<()> {
    let shutdown_notify = Arc::new(Notify::new());
    let config = RedisConfig::from_url(&ENV_CONFIG.redis_uri)?;
    let redis_pool = RedisPool::new(config, None, None, 4)?;
    redis_pool.connect();
    redis_pool
        .wait_for_connect()
        .await
        .context("failed to connect to redis")?;
    let redis_consumer_health = health::RedisConsumerHealth::new();

    let (submissions_tx, submissions_rx) = channel(4);

    consumer::run_consume_submissions_thread(
        redis_pool.clone(),
        redis_consumer_health.clone(),
        shutdown_notify.clone(),
        submissions_tx.clone(),
    );

    storage::run_store_submissions_thread(
        redis_pool.clone(),
        submissions_rx,
        shutdown_notify.clone(),
    );

    let block_submission = {
        let file = std::fs::File::open("tests/fixtures/0x003fc7725acd9365420171d5768dda1d0dd28dc50ea15126f4a4db81b2cf3dfb.json.gz.decompressed")?;
        let submission: BlockSubmission = serde_json::from_reader(file)?;
        submission
    };

    let block_submission_key = block_submission.block_submission_key().to_string();
    let block_hash = block_submission.block_hash();
    let pairs: MultipleOrderedPairs = block_submission.try_into()?;

    redis_pool.xadd(STREAM_NAME, true, None, "*", pairs).await?;

    // Give our threads a moment to process the new block submission.
    sleep(Duration::from_millis(200)).await;

    let stored_submission: RedisValue = redis_pool.get(block_submission_key).await?;

    assert!(stored_submission.ne(&RedisValue::Null));

    let stored_submission = match stored_submission {
        RedisValue::String(s) => s,
        _ => panic!("expected stored submission to be a string"),
    };
    let stored_submission: JsonValue = serde_json::from_str(&stored_submission)?;

    assert_eq!(stored_submission["block_hash"], block_hash);

    Ok(())
}
