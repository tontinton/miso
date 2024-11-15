use std::pin::Pin;

use color_eyre::Result;
use futures_core::Stream;
use vrl::value::ObjectMap;

pub type Log = ObjectMap;
pub type LogStream = Pin<Box<dyn Stream<Item = Log> + Send>>;
pub type LogTryStream = Pin<Box<dyn Stream<Item = Result<Log>> + Send>>;
