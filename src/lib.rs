pub mod periodic;
pub mod sliding;
pub mod tumbling;

use futures::StreamExt;
use periodic::to_periodic_window;
use sliding::to_sliding_window;
use std::pin::Pin;
use tokio_stream::Stream;
use tumbling::to_tumbling_window;

/// Wrappers for actual window implementations, as an extension trait.
pub trait WindowExt: Stream {
    fn tumbling_window<'a>(
        self,
        window_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Vec<Self::Item>> + 'a>>
    where
        Self: Unpin + Sized + 'a,
        Self::Item: Clone,
    {
        to_tumbling_window(self, window_size).boxed_local()
    }

    fn tumbling_window_unpin<'a>(
        self,
        window_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Vec<Self::Item>> + 'a>>
    where
        Self: Sized + 'a,
        Self::Item: Clone,
    {
        to_tumbling_window(Box::pin(self), window_size).boxed_local()
    }

    fn sliding_window<'a>(
        self,
        window_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Vec<Self::Item>> + 'a>>
    where
        Self: Unpin + Sized + 'a,
        Self::Item: Clone,
    {
        to_sliding_window(self, window_size).boxed_local()
    }

    fn sliding_window_unpin<'a>(
        self,
        window_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Vec<Self::Item>> + 'a>>
    where
        Self: Sized + 'a,
        Self::Item: Clone,
    {
        to_sliding_window(Box::pin(self), window_size).boxed_local()
    }

    fn periodic_window<'a, CT>(
        self,
        clock_stream: impl Stream<Item = CT> + Unpin + 'a,
        emit_last: bool,
    ) -> Pin<Box<dyn Stream<Item = Vec<Self::Item>> + 'a>>
    where
        Self: Unpin + Sized + 'a,
    {
        to_periodic_window(self, clock_stream, emit_last).boxed_local()
    }

    fn periodic_window_unpin<'a, CT>(
        self,
        clock_stream: impl Stream<Item = CT> + 'a,
        emit_last: bool,
    ) -> Pin<Box<dyn Stream<Item = Vec<Self::Item>> + 'a>>
    where
        Self: Sized + 'a,
    {
        to_periodic_window(Box::pin(self), Box::pin(clock_stream), emit_last).boxed_local()
    }
}

impl<S> WindowExt for S where S: Stream {}

#[cfg(test)]
mod tests {
    use super::*;
    use async_stream::stream;
    use futures::StreamExt;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::time::{interval_at, sleep, Instant};
    use tokio_stream::wrappers::IntervalStream;

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
}
