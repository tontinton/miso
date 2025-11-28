use time::OffsetDateTime;

pub fn parse_timestamp(timestamp: i64) -> Result<OffsetDateTime, String> {
    const MIN_TIMESTAMP_SECONDS: i64 = 72_057_595;
    const MAX_TIMESTAMP_SECONDS: i64 = 8_589_934_591;

    const MIN_TIMESTAMP_MILLIS: i64 = MIN_TIMESTAMP_SECONDS * 1000;
    const MAX_TIMESTAMP_MILLIS: i64 = MAX_TIMESTAMP_SECONDS * 1000;
    const MIN_TIMESTAMP_MICROS: i64 = MIN_TIMESTAMP_SECONDS * 1_000_000;
    const MAX_TIMESTAMP_MICROS: i64 = MAX_TIMESTAMP_SECONDS * 1_000_000;
    const MIN_TIMESTAMP_NANOS: i64 = MIN_TIMESTAMP_SECONDS * 1_000_000_000;
    const MAX_TIMESTAMP_NANOS: i64 = MAX_TIMESTAMP_SECONDS * 1_000_000_000;

    match timestamp {
        MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS => {
            OffsetDateTime::from_unix_timestamp(timestamp)
                .map_err(|e| format!("failed to parse unix timestamp seconds: {}", e))
        }
        MIN_TIMESTAMP_MILLIS..=MAX_TIMESTAMP_MILLIS => {
            OffsetDateTime::from_unix_timestamp_nanos(timestamp as i128 * 1_000_000)
                .map_err(|e| format!("failed to parse unix timestamp millis: {}", e))
        }
        MIN_TIMESTAMP_MICROS..=MAX_TIMESTAMP_MICROS => {
            OffsetDateTime::from_unix_timestamp_nanos(timestamp as i128 * 1_000)
                .map_err(|e| format!("failed to parse unix timestamp micros: {}", e))
        }
        MIN_TIMESTAMP_NANOS..=MAX_TIMESTAMP_NANOS => {
            OffsetDateTime::from_unix_timestamp_nanos(timestamp as i128)
                .map_err(|e| format!("failed to parse unix timestamp nanos: {}", e))
        }
        _ => Err(format!(
            "failed to parse unix timestamp `{timestamp}`. Supported timestamp ranges \
             from `13 Apr 1972 23:59:55` to `16 Mar 2242 12:56:31`"
        )),
    }
}

pub fn parse_timestamp_float(timestamp: f64) -> Result<OffsetDateTime, String> {
    const MIN_TIMESTAMP_SECONDS: f64 = 72_057_595.0;
    const MAX_TIMESTAMP_SECONDS: f64 = 8_589_934_591.0;

    const MIN_TIMESTAMP_MILLIS: f64 = MIN_TIMESTAMP_SECONDS * 1000.0;
    const MAX_TIMESTAMP_MILLIS: f64 = MAX_TIMESTAMP_SECONDS * 1000.0;
    const MIN_TIMESTAMP_MICROS: f64 = MIN_TIMESTAMP_SECONDS * 1_000_000.0;
    const MAX_TIMESTAMP_MICROS: f64 = MAX_TIMESTAMP_SECONDS * 1_000_000.0;

    if (MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS).contains(&timestamp) {
        let whole = timestamp.trunc();
        let fract = timestamp.fract();
        let nanos = (whole as i128 * 1_000_000_000) + (fract * 1_000_000_000.0) as i128;
        OffsetDateTime::from_unix_timestamp_nanos(nanos)
            .map_err(|e| format!("failed to parse unix timestamp seconds: {}", e))
    } else if (MIN_TIMESTAMP_MILLIS..=MAX_TIMESTAMP_MILLIS).contains(&timestamp) {
        let whole = timestamp.trunc();
        let fract = timestamp.fract();
        let nanos = (whole as i128 * 1_000_000) + (fract * 1_000_000.0) as i128;
        OffsetDateTime::from_unix_timestamp_nanos(nanos)
            .map_err(|e| format!("failed to parse unix timestamp millis: {}", e))
    } else if (MIN_TIMESTAMP_MICROS..=MAX_TIMESTAMP_MICROS).contains(&timestamp) {
        let whole = timestamp.trunc();
        let fract = timestamp.fract();
        let nanos = (whole as i128 * 1_000) + (fract * 1_000.0) as i128;
        OffsetDateTime::from_unix_timestamp_nanos(nanos)
            .map_err(|e| format!("failed to parse unix timestamp micros: {}", e))
    } else {
        Err(format!(
            "failed to parse unix timestamp `{timestamp}`. Supported timestamp ranges \
             from `13 Apr 1972 23:59:55` to `16 Mar 2242 12:56:31`"
        ))
    }
}
