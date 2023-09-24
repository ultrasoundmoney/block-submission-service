//! # Operational constants
//! Sets important configuration constants for the operation of this service.
//!
//! How many block submissions should the service keep in memory to be able to serve requests from
//! proposers to the proposer-api, for block submissions which the relay already dropped in favor
//! of better bids?
//!
//! Let's work through an example: slot bidding starts prior to the slot and ends at the latest
//! about 3s into the slot. To be safe, take 1.5 slots of range. As an example, let's say our relay
//! receives at most 2000 bids per slot. Because it's hard to be confident in the exact number or
//! predict its variance in the future, add a buffer factor to your liking, e.g. 2. In this
//! scenario we would want to keep 2000 * 1.5 * 3 = 9000 block submissions in memory.
const MAX_SUBMISSIONS_PER_SLOT: usize = 2000;
const SLOT_RANGE: f64 = 1.5;
const BUFFER_FACTOR: usize = 3;
pub const BLOCK_SUBMISSION_CACHE_SIZE: usize =
    (MAX_SUBMISSIONS_PER_SLOT as f64 * SLOT_RANGE * BUFFER_FACTOR as f64) as usize;
