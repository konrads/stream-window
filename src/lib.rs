mod periodic;
mod sliding;
mod tumbling;

use std::pin::Pin;

use futures::StreamExt;
use periodic::{to_periodic_window, PeriodicWindow};
use sliding::SlidingWindow;
use tokio_stream::Stream;
use tumbling::TumblingWindow;

pub trait WindowExt: Stream {
    fn tumbling_window(self, window_size: usize) -> TumblingWindow<Self::Item, Self>
    where
        Self: Sized,
    {
        TumblingWindow::new(window_size, self)
    }

    fn sliding_window(self, window_size: usize) -> SlidingWindow<Self::Item, Self>
    where
        Self: Sized,
    {
        SlidingWindow::new(window_size, self)
    }

    /// Window bounded by shorter `stream``, ie. will stop when `stream`'s window ends.
    fn periodic_window<CT>(
        self,
        clock_stream: impl Stream<Item = CT> + Send + 'static,
    ) -> PeriodicWindow<Self::Item>
    where
        Self: Send + Sized + 'static,
    {
        PeriodicWindow::new(self, clock_stream)
    }

    fn periodic_window2<'a, CT>(
        self,
        clock_stream: impl Stream<Item = CT> + Unpin + 'a,
        emit_last: bool,
    ) -> Pin<Box<dyn Stream<Item = Vec<Self::Item>> + 'a>>
    where
        Self: Unpin + Sized + 'a,
    {
        to_periodic_window(self, clock_stream, emit_last).boxed_local()
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
        let window = stream.tumbling_window(3);
        let res = window.collect::<Vec<Vec<i32>>>().await;
        assert_eq!(
            res,
            vec![vec![11, 22, 33], vec![44, 55, 66], vec![77, 88, 99]]
        )
    }

    #[tokio::test]
    async fn test_sliding_window() {
        let stream = tokio_stream::iter(vec![11, 22, 33, 44, 55, 66, 77, 88, 99, 100]);
        let window = stream.sliding_window(4);
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

    /// Tests both the `periodic_window` and the fact that the `stream` is bounded and `clock_stream` unbounded
    /// and yet the `periodic_window` ends with the last window emit.
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

        let stream = stream! {
            for d in delays {
                let delay = t0 + d - now();
                sleep(Duration::from_millis(delay as u64)).await;
                yield d;
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
            // to_periodic_window(&mut std::pin::pin!(stream), &mut std::pin::pin!(clock_stream), false)
            stream
                .periodic_window(clock_stream)
                .collect::<Vec<_>>()
                .await
        );
    }

    #[tokio::test]
    async fn test_periodic2() {
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
            // to_periodic_window(std::pin::pin!(stream), std::pin::pin!(clock_stream), true)
            Box::pin(stream)
                .periodic_window2(Box::pin(clock_stream), true)
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
