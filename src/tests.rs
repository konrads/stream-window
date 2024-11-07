use crate::{merge::merge, to_periodic_window, WindowExt};
use async_stream::stream;
use futures_channel::mpsc;
use futures_util::{task, SinkExt, StreamExt};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{interval_at, sleep, Instant};
use tokio_stream::wrappers::IntervalStream;
use tokio_test::{assert_pending, assert_ready, assert_ready_eq};

#[tokio::test]
async fn test_tumbling_window() {
    let stream = tokio_stream::iter(vec![11, 22, 33, 44, 55, 66, 77, 88, 99, 100]);
    let window = stream.tumbling_window_unpin(3);
    let res = window.collect::<Vec<Vec<i32>>>().await;
    assert_eq!(
        res,
        vec![vec![11, 22, 33], vec![44, 55, 66], vec![77, 88, 99]]
    )
}

#[tokio::test]
async fn test_sliding_window() {
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
}

#[tokio::test]
async fn test_periodic() {
    let clock_freq = Duration::from_millis(100);
    let start = Instant::now() + clock_freq;
    let clock_stream = IntervalStream::new(interval_at(start, clock_freq)); // bounded by the size of `clock_stream`

    // delays grouped per 100ms tick
    let delays = vec![
        20, 30, 40, 50, 60, // tick 0
        120, 150, // tick 1
        220, 230, 240, 250, 260, 270, // tick 2
        350, // tick 4
        450, 460, 470, // tick 5
        550, // tick 6 - emitted at the stream end, ie. not at final clock tick
    ];
    let t0 = now();

    let delays_iter = delays.iter();
    let stream = stream! {
        for d in delays_iter {
            let delay = t0 + d - now();
            sleep(Duration::from_millis(delay as u64)).await;
            yield *d;
        }
    };

    assert_eq!(
        vec![
            vec![20, 30, 40, 50, 60],           // tick 0
            vec![120, 150],                     // tick 1
            vec![220, 230, 240, 250, 260, 270], // tick 2
            vec![350],                          // tick 4
            vec![450, 460, 470],                // tick 5
            vec![550],                          // tick 6, emitted prior to next clock tick
        ],
        stream
            .periodic_window_unpin(clock_stream, true)
            .collect::<Vec<_>>()
            .await
    );

    // for compilation verification, with std::pin::pin!() macro
    let clock_stream = IntervalStream::new(interval_at(start, clock_freq));
    let stream = tokio_stream::iter(delays);
    let _ = to_periodic_window(std::pin::pin!(stream), std::pin::pin!(clock_stream), true);
    // NO FURTHER VALIDATION
}

fn now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

#[tokio::test]
async fn test_merge_streams() {
    #[derive(Debug, PartialEq)]
    enum OneOf3<T1, T2, T3> {
        One(T1),
        Two(T2),
        Three(T3),
    }

    let waker = task::noop_waker_ref();
    let mut cx = std::task::Context::from_waker(waker);

    let (mut tx1, rx1) = mpsc::unbounded();
    let (mut tx2, rx2) = mpsc::unbounded();
    let (mut tx3, rx3) = mpsc::unbounded();

    let mut merged = Box::pin(merge!(
        rx1.map(OneOf3::One),
        rx2.map(OneOf3::Two),
        rx3.map(OneOf3::Three)
    ));

    assert_pending!(merged.poll_next_unpin(&mut cx));

    tx1.send(1).await.unwrap();
    assert_ready_eq!(merged.poll_next_unpin(&mut cx), Some(OneOf3::One(1)));
    assert_pending!(merged.poll_next_unpin(&mut cx));

    tx2.send("A").await.unwrap();
    assert_ready_eq!(merged.poll_next_unpin(&mut cx), Some(OneOf3::Two("A")));
    assert_pending!(merged.poll_next_unpin(&mut cx));

    tx2.send("B").await.unwrap();
    assert_ready_eq!(merged.poll_next_unpin(&mut cx), Some(OneOf3::Two("B")));
    assert_pending!(merged.poll_next_unpin(&mut cx));

    tx3.send(-1).await.unwrap();
    assert_ready_eq!(merged.poll_next_unpin(&mut cx), Some(OneOf3::Three(-1)));
    assert_pending!(merged.poll_next_unpin(&mut cx));

    tx2.send("C").await.unwrap();
    assert_ready_eq!(merged.poll_next_unpin(&mut cx), Some(OneOf3::Two("C")));
    assert_pending!(merged.poll_next_unpin(&mut cx));

    tx1.send(2).await.unwrap();
    assert_ready_eq!(merged.poll_next_unpin(&mut cx), Some(OneOf3::One(2)));
    assert_pending!(merged.poll_next_unpin(&mut cx));

    tx3.send(-2).await.unwrap();
    assert_ready_eq!(merged.poll_next_unpin(&mut cx), Some(OneOf3::Three(-2)));
    assert_pending!(merged.poll_next_unpin(&mut cx));

    tx1.send(3).await.unwrap();
    tx2.send("D").await.unwrap();
    tx3.send(-3).await.unwrap();
    tx1.send(666).await.unwrap();

    // Either 3, "D" or -3 will be next - the order doesn't matter. But we definitely
    // don't want 666 before "D" - it should at least alternate polling of both streams
    // so that we don't have one stream starving the other!
    let first = assert_ready!(merged.poll_next_unpin(&mut cx));
    assert!(matches!(
        first,
        Some(OneOf3::One(3)) | Some(OneOf3::Two("D")) | Some(OneOf3::Three(-3))
    ));

    let second = assert_ready!(merged.poll_next_unpin(&mut cx));
    assert_ne!(first, second);
    assert!(matches!(
        second,
        Some(OneOf3::One(3)) | Some(OneOf3::Two("D")) | Some(OneOf3::Three(-3))
    ));

    let third = assert_ready!(merged.poll_next_unpin(&mut cx));
    assert_ne!(first, third);
    assert_ne!(second, third);
    assert!(matches!(
        third,
        Some(OneOf3::One(3)) | Some(OneOf3::Two("D")) | Some(OneOf3::Three(-3))
    ));

    assert_ready_eq!(merged.poll_next_unpin(&mut cx), Some(OneOf3::One(666)));
    assert_pending!(merged.poll_next_unpin(&mut cx));

    // Only one of the streams needs to terminate for the zipped stream to end.
    drop(tx1);
    assert_ready_eq!(merged.poll_next_unpin(&mut cx), None);
}
