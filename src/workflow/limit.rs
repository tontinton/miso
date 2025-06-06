use async_stream::try_stream;
use futures_util::StreamExt;

use crate::log::{LogStream, LogTryStream};

pub fn limit_stream(limit: u32, mut input_stream: LogStream) -> LogTryStream {
    Box::pin(try_stream! {
        let mut streamed = 0;
        while let Some(log) = input_stream.next().await {
            if streamed >= limit {
                break;
            }
            yield log;
            streamed += 1;
        }
    })
}
