use std::collections::HashMap;

use anyhow::{Context, Result};
use bytes::Bytes;
use fred::{
    prelude::{RedisError, RedisErrorKind},
    types::{FromRedis, MultipleOrderedPairs, RedisKey, RedisMap, RedisValue},
};
use serde::{Deserialize, Serialize};

use crate::{BlockSubmissionKey, Slot};

/// Block submission archive entries.
/// These are block submissions as they came in on the relay, plus some metadata.
#[derive(Deserialize, Serialize)]
pub struct BlockSubmission {
    eligible_at: i64,
    pub payload: serde_json::Value,
    received_at: u64,
    status_code: Option<u16>,
}

impl std::fmt::Debug for BlockSubmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state_root = self.state_root();
        f.debug_struct("BlockSubmission")
            .field("eligible_at", &self.eligible_at)
            .field("payload", &format!("<PAYLOAD_JSON:{state_root}>"))
            .field("received_at", &self.received_at)
            .finish()
    }
}

impl From<BlockSubmission> for MultipleOrderedPairs {
    fn from(entry: BlockSubmission) -> Self {
        let pairs: Vec<(String, String)> = vec![
            ("eligible_at".into(), entry.eligible_at.to_string()),
            ("payload".into(), entry.payload.to_string()),
            ("received_at".into(), entry.received_at.to_string()),
        ];
        pairs.try_into().unwrap()
    }
}

impl FromRedis for BlockSubmission {
    fn from_value(value: RedisValue) -> Result<Self, RedisError> {
        let mut map: HashMap<String, Bytes> = value.convert()?;
        let eligible_at = {
            let bytes = map
                .remove("eligible_at")
                .expect("expect eligible_at in block submission")
                .to_vec();
            let str = String::from_utf8(bytes)?;
            str.parse::<i64>()?
        };
        let received_at = {
            let bytes = map
                .remove("received_at")
                .expect("expect received_at in block submission")
                .to_vec();
            let str = String::from_utf8(bytes)?;
            str.parse::<u64>()?
        };
        let payload = {
            let bytes = map
                .remove("payload")
                .expect("expect payload in block submission")
                .to_vec();
            // We could implement custom Deserialize for this to avoid parsing the JSON here, we
            // don't do anything with it besides Serialize it later.
            serde_json::from_slice(&bytes)
                .context("failed to parse block submission payload as JSON")
                .map_err(|e| RedisError::new(RedisErrorKind::Parse, e.to_string()))?
        };
        let status_code = {
            let status_code_bytes = map.remove("status_code");
            // Until the builder-api is updated to match we allow this to be missing.
            match status_code_bytes {
                None => None,
                Some(bytes) => {
                    let str = String::from_utf8(bytes.to_vec())?;
                    let status_code = str.parse::<u16>()?;
                    Some(status_code)
                }
            }
        };
        Ok(Self {
            eligible_at,
            payload,
            received_at,
            status_code,
        })
    }
}

impl From<BlockSubmission> for RedisValue {
    fn from(entry: BlockSubmission) -> Self {
        let mut map: HashMap<RedisKey, RedisValue> = HashMap::new();
        map.insert("eligible_at".into(), RedisValue::Integer(entry.eligible_at));
        map.insert(
            "payload".into(),
            RedisValue::String(entry.payload.to_string().into()),
        );
        map.insert(
            "received_at".into(),
            RedisValue::Integer(entry.received_at as i64),
        );
        if let Some(status_code) = entry.status_code {
            map.insert(
                "status_code".into(),
                RedisValue::Integer(status_code.into()),
            );
        }
        RedisMap::try_from(map).map(RedisValue::Map).unwrap()
    }
}

impl BlockSubmission {
    pub fn new(
        eligible_at: i64,
        payload: serde_json::Value,
        received_at: u64,
        status_code: u16,
    ) -> Self {
        Self {
            eligible_at,
            payload,
            received_at,
            status_code: Some(status_code),
        }
    }

    pub fn block_hash(&self) -> String {
        self.payload["message"]["block_hash"]
            .as_str()
            .unwrap()
            .to_string()
    }

    pub fn proposer_pubkey(&self) -> String {
        self.payload["message"]["proposer_pubkey"]
            .as_str()
            .unwrap()
            .to_string()
    }

    pub fn slot(&self) -> Slot {
        let slot_str = self.payload["message"]["slot"].as_str().unwrap();
        slot_str.parse::<i32>().unwrap()
    }

    fn state_root(&self) -> String {
        self.payload["execution_payload"]["state_root"]
            .as_str()
            .unwrap()
            .to_string()
    }

    pub fn block_submission_key(&self) -> BlockSubmissionKey {
        let slot = self.slot();
        let proposer_pubkey = self.proposer_pubkey();
        let block_hash = self.block_hash();
        BlockSubmissionKey::new(slot, proposer_pubkey, block_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use fred::types::{RedisMap, RedisValue};
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn create_block_submission() {
        let payload =
            json!({"message": {"slot": "42"}, "execution_payload": {"state_root": "some_root"}});
        let submission = BlockSubmission::new(100, payload.clone(), 200, 400);

        assert_eq!(submission.eligible_at, 100);
        assert_eq!(submission.payload, payload);
        assert_eq!(submission.received_at, 200);
        assert_eq!(submission.status_code, Some(400));
    }

    #[test]
    fn test_block_submission_from_redis() {
        let mut map = HashMap::new();
        map.insert("eligible_at".to_string(), Bytes::from("100"));
        map.insert("payload".to_string(), Bytes::from("{\"message\": {\"slot\": \"42\"}, \"execution_payload\": {\"state_root\": \"some_root\"}}"));
        map.insert("received_at".to_string(), Bytes::from("200"));
        map.insert("status_code".to_string(), Bytes::from("400"));

        let mut redis_map = RedisMap::new();
        redis_map.insert("eligible_at".into(), RedisValue::String("100".into()));
        redis_map.insert(
            "payload".into(),
            RedisValue::String(
                "{\"message\": {\"slot\": \"42\"}, \"execution_payload\": {\"state_root\": \"some_root\"}}"
                    .into(),
            ),
        );
        redis_map.insert("received_at".into(), RedisValue::String("200".into()));
        redis_map.insert("status_code".into(), RedisValue::String("400".into()));
        let value: RedisValue = RedisValue::Map(redis_map);
        let result = BlockSubmission::from_value(value);

        assert!(result.is_ok());
        let submission = result.unwrap();
        assert_eq!(submission.eligible_at, 100);
        assert_eq!(submission.received_at, 200);
        assert_eq!(submission.status_code, Some(400));
    }
}
