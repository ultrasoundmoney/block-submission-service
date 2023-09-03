//! # Submission Service
//!
//! Keeps a cache of most recently seen block submissions, and offers them over an API.
//!
//! ## Configuration
//! The most important configuration parameter for this service is how many submissions you'd like it to store.
//!
//! ## Improvements
//!
//! An obvious improvement would be to keep track of bid values across submissions for a given slot
//! and start dropping submissions that are far below current bid value.

fn main() {
    println!("hello from submission service");
}
