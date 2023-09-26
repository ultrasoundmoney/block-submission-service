use block_submission_service::BlockSubmission;
use fred::{
    prelude::{RedisError, RedisErrorKind},
    types::{FromRedis, RedisValue},
};
use tracing::error;

fn into_redis_parse_err(err: impl std::fmt::Display) -> RedisError {
    RedisError::new(RedisErrorKind::Parse, err.to_string())
}

pub struct XReadBlockSubmissions(pub Option<Vec<(String, BlockSubmission)>>);

impl FromRedis for XReadBlockSubmissions {
    fn from_value(value: RedisValue) -> Result<Self, RedisError> {
        match value {
            RedisValue::Null => Ok(XReadBlockSubmissions(None)),
            RedisValue::Array(streams) => {
                let first_stream = streams.into_iter().next().ok_or_else(|| {
                    into_redis_parse_err(
                        "expect first element in streams array to be the submissions stream",
                    )
                })?;

                // The stream is an array of two elements. The first is the stream name, the second
                // is an array of messages.
                let mut submissions_stream = first_stream.into_array().into_iter();
                let _name = submissions_stream.next();
                let key_value_pairs = submissions_stream.next().ok_or_else(|| {
                    into_redis_parse_err(
                        "expect second element in the submissions stream array to be a RedisValue::Array with pairs of message keys and values",
                    )
                })?.into_array();

                key_value_pairs
                    .into_iter()
                    .map(|key_value_pair| {
                        let mut key_value_pair_iter = key_value_pair.into_array().into_iter();
                        let key = key_value_pair_iter
                            .next()
                            .and_then(|key| key.into_string())
                            .ok_or_else(|| {
                                into_redis_parse_err(
                                    "expect first element in key value pair to be a key",
                                )
                            })?;
                        let value = key_value_pair_iter
                            .next()
                            .ok_or_else(|| {
                                into_redis_parse_err(
                                "expect second element in key value pair to be a block submission",
                            )
                            })?
                            .convert::<BlockSubmission>()?;

                        Ok((key, value))
                    })
                    .collect::<Result<Vec<_>, RedisError>>()
                    .map(Some)
                    .map(XReadBlockSubmissions)
            }
            response => {
                error!(response=?response, "expect XREAD response to be nil or an array");
                Err(into_redis_parse_err(
                    "expect XREAD response to be nil or an array",
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn mock_valid_redis_value() -> RedisValue {
        RedisValue::Array(vec![RedisValue::Array(vec![
            RedisValue::String("stream_name".into()),
            RedisValue::Array(vec![RedisValue::Array(vec![
                RedisValue::String("key".into()),
                BlockSubmission::new(0, json!({"some": "data"}), 0, 200).into(),
            ])]),
        ])])
    }

    fn mock_invalid_redis_value() -> RedisValue {
        RedisValue::Array(vec![RedisValue::Array(vec![
            RedisValue::String("stream_name".into()),
            RedisValue::String("invalid_data".into()),
        ])])
    }

    #[test]
    fn from_redis_value_null() {
        let value = RedisValue::Null;
        let x_read_response = XReadBlockSubmissions::from_value(value).unwrap();
        assert!(x_read_response.0.is_none());
    }

    #[test]
    fn from_redis_value_valid_array() {
        let value = mock_valid_redis_value();
        let x_read_response = XReadBlockSubmissions::from_value(value).unwrap();

        assert!(x_read_response.0.is_some());
        let messages = x_read_response.0.unwrap();

        assert_eq!(messages.len(), 1);
        let (ref key, ref _block_submission) = messages[0];
        assert_eq!(key, "key");
    }

    #[test]
    fn from_redis_value_invalid_array() {
        let value = mock_invalid_redis_value();
        let result = XReadBlockSubmissions::from_value(value);

        assert!(result.is_err());
    }
}
