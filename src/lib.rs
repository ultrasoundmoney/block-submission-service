mod block_submission_key;
mod block_submissions;
mod consumer;
pub mod env;
mod health;
pub mod log;
pub mod performance;
mod server;
mod storage;

pub use block_submission_key::BlockSubmissionKey;
pub use block_submissions::BlockSubmission;
pub use consumer::run_consume_submissions_thread;
pub use health::RedisConsumerHealth;
pub use health::RedisHealth;
pub use server::run_server_thread;
pub use server::AppState;
pub use storage::run_store_submissions_thread;

pub type JsonValue = serde_json::value::Value;

type Slot = i32;

pub const STREAM_NAME: &str = "block-submission-archive";
