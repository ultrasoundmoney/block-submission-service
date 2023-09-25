mod block_submission_key;
mod block_submissions;
pub mod env;
pub mod log;

pub use block_submission_key::BlockSubmissionKey;
pub use block_submissions::BlockSubmission;

pub type JsonValue = serde_json::value::Value;

type Slot = i32;

pub const STREAM_NAME: &str = "block-submission-archive";
