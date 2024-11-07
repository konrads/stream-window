use futures_util::{stream, Stream, StreamExt};
use std::{convert, future};

#[macro_export]
macro_rules! merge {
    ($first:expr) => {
        $first
    };

    ($first:expr, $($rest:expr),+) => {
        $crate::merge::merge2($first, merge!($($rest),+))
    };
}

pub use merge;

/// As for [merge](tokio_stream::StreamExt::merge) but terminates when either stream terminates.
pub fn merge2<S1, S2>(stream1: S1, stream2: S2) -> impl Stream<Item = S1::Item>
where
    S1: Stream,
    S2: Stream<Item = S1::Item>,
{
    let stream1 = stream1.map(Some).chain(stream::once(future::ready(None)));
    let stream2 = stream2.map(Some).chain(stream::once(future::ready(None)));

    let combined = tokio_stream::StreamExt::merge(stream1, stream2);
    tokio_stream::StreamExt::map_while(combined, convert::identity)
}
