mod block_submission_key;
mod block_submissions;
pub mod consumer;
pub mod env;
pub mod health;
pub mod log;
pub mod server;
pub mod storage;

pub use block_submission_key::BlockSubmissionKey;
pub use block_submissions::BlockSubmission;

pub type JsonValue = serde_json::value::Value;

type Slot = i32;

pub const STREAM_NAME: &str = "block-submission-archive";
