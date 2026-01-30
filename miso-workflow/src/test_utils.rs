use miso_workflow_types::{
    log::{Log, LogItem, PartialStreamKey},
    value::Value,
};

pub fn log() -> LogItem {
    LogItem::Log(Log::new())
}

pub fn make_log(x: i64) -> Log {
    let mut log = Log::new();
    log.insert("x".into(), Value::from(x));
    log
}

pub fn plog(log: Log, partial_stream_id: usize, source_id: usize) -> LogItem {
    LogItem::PartialStreamLog(
        log,
        PartialStreamKey {
            partial_stream_id,
            source_id,
        },
    )
}

pub fn plog_empty(partial_stream_id: usize, source_id: usize) -> LogItem {
    plog(Log::new(), partial_stream_id, source_id)
}

pub fn plog_val(val: i64, partial_stream_id: usize, source_id: usize) -> LogItem {
    plog(make_log(val), partial_stream_id, source_id)
}

pub fn pdone(partial_stream_id: usize, source_id: usize) -> LogItem {
    LogItem::PartialStreamDone(PartialStreamKey {
        partial_stream_id,
        source_id,
    })
}
