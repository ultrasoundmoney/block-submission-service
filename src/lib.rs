mod block_submissions;
pub mod env;
pub mod log;

use std::{fmt::Display, str::FromStr};

use anyhow::anyhow;
pub use block_submissions::BlockSubmission;

pub type JsonValue = serde_json::value::Value;

pub const STREAM_NAME: &str = "block-submission-archive";

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct BlockSubmissionKey {
    block_hash: BlockHash,
    proposer_pubkey: ProposerPubkey,
    slot: Slot,
}

type Slot = i32;
type ProposerPubkey = String;
type BlockHash = String;

impl BlockSubmissionKey {
    pub fn new(slot: Slot, proposer_pubkey: ProposerPubkey, block_hash: BlockHash) -> Self {
        Self {
            block_hash,
            proposer_pubkey,
            slot,
        }
    }
}

impl FromStr for BlockSubmissionKey {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split('_');
        let slot = parts.next().ok_or_else(|| anyhow!("missing slot"))?;
        let proposer_pubkey = parts
            .next()
            .ok_or_else(|| anyhow!("missing proposer_pubkey"))?;
        let block_hash = parts.next().ok_or_else(|| anyhow!("missing block_hash"))?;

        Ok(Self {
            block_hash: block_hash.to_string(),
            proposer_pubkey: proposer_pubkey.to_string(),
            slot: slot.parse::<i32>()?,
        })
    }
}

impl Display for BlockSubmissionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}_{}_{}",
            self.slot, self.proposer_pubkey, self.block_hash
        )
    }
}
