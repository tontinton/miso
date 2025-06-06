use async_stream::try_stream;
use futures_util::{stream, StreamExt};
use hashbrown::HashMap;

use crate::log::{LogStream, LogTryStream};

use super::partial_stream::get_partial_id;

pub fn limit_stream(limit: u32, mut input_stream: LogStream) -> LogTryStream {
    if limit == 0 {
        return Box::pin(stream::empty());
    }

    let mut streamed = 0;
    let mut partial_limits: HashMap<usize, Option<u32>> = HashMap::new();

    Box::pin(try_stream! {
        while let Some(log) = input_stream.next().await {
            let partial_result = get_partial_id(&log);

            yield log;

            match partial_result {
                None => {
                    streamed += 1;
                    if streamed >= limit {
                        break;
                    }
                }
                Some((id, false)) => {
                    let entry = partial_limits.entry(id).or_insert(Some(0));
                    if let Some(active_entry) = entry {
                        *active_entry += 1;
                        if *active_entry >= limit {
                            *entry = None;
                        }
                    }
                }
                Some((id, true)) => {
                    if let Some(entry) = partial_limits.get_mut(&id) {
                        *entry = None;
                    }
                }
            }
        }
    })
}
