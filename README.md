# Stream windows

[![build](../../workflows/build/badge.svg)](../../actions/workflows/build.yml)

Window implementations for async streams:

- [x] tumbling
- [x] sliding
- [x] periodic (requires a clock stream, emits window on every `tick`)

## Usage

### Tumbling window

```rust
use stream_window::WindowExt;

let stream = tokio_stream::iter(vec![11, 22, 33, 44, 55, 66, 77, 88, 99, 100]);
let window = stream.tumbling_window_unpin(3);
let res = window.collect::<Vec<Vec<i32>>>().await;
assert_eq!(res, vec![vec![11, 22, 33], vec![44, 55, 66], vec![77, 88, 99]]);
```

### Sliding window

```rust
use stream_window::WindowExt;

let stream = tokio_stream::iter(vec![11, 22, 33, 44, 55, 66, 77, 88, 99, 100]);
let window = stream.sliding_window_unpin(4);
let res = window.collect::<Vec<Vec<i32>>>().await;
assert_eq!(
    res,
    vec![
        vec![11, 22, 33, 44],
        vec![22, 33, 44, 55],
        vec![33, 44, 55, 66],
        vec![44, 55, 66, 77],
        vec![55, 66, 77, 88],
        vec![66, 77, 88, 99],
        vec![77, 88, 99, 100]
    ]
)
```

### Periodic window

```rust
use stream_window::WindowExt;
use async_stream::stream;
use futures::StreamExt;
use std::time::Duration;
use tokio::time::{interval_at, sleep, Instant};
use tokio_stream::wrappers::IntervalStream;

let clock_freq = Duration::from_millis(100);
let start = Instant::now() + clock_freq;
let clock_stream = IntervalStream::new(interval_at(start, clock_freq));
let stream = stream! {
    for d in delays {
        sleep(Duration::from_millis(d)).await;
        yield d;
    }
};
let windows = stream.periodic_window_unpin(clock_stream, true).collect::<Vec<_>>().await;
```
