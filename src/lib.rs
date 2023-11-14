mod periodic;
mod sliding;
mod tumbling;

use periodic::PeriodicWindow;
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

    /// Window isn't bounded by shorter `stream``, ie. will continue emitting empty if `stream` ends prior to `clock_stream`.
    fn periodic_window<CT>(
        self,
        clock_stream: impl Stream<Item = CT> + Send + 'static,
    ) -> PeriodicWindow<Self::Item>
    where
        Self: Send + Sized + 'static,
    {
        PeriodicWindow::new(self, clock_stream)
    }

    /// Window bounded by shorter `stream``, ie. will stop when `stream`'s window ends.
    fn periodic_window_bounded<CT>(
        self,
        clock_stream: impl Stream<Item = CT> + Send + 'static,
    ) -> PeriodicWindow<Self::Item>
    where
        Self: Send + Sized + 'static,
    {
        PeriodicWindow::new_bounded(self, clock_stream)
    }
}

impl<S> WindowExt for S where S: Stream {}

#[cfg(test)]
mod tests {
    use super::*;
    use async_stream::stream;
    use futures::StreamExt;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::time::{sleep, Instant};

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

    /// Tests both the `periodic_window`.
    #[tokio::test]
    async fn test_periodic_window() {
        let clock_freq = Duration::from_millis(100);
        let start = Instant::now() + clock_freq;
        // note, unbounded clock_stream, ie. unrestricted by input stream size
        let clock_stream = futures::stream::unfold(
            (tokio::time::interval_at(start, clock_freq), 0),
            |(mut interval, cnt)| async move {
                interval.tick().await;
                Some((cnt, (interval, cnt + 1)))
            },
        )
        .take(6) // bounded by the size of `clock_stream`
        .boxed();

        // delays grouped per 100ms tick
        let delays = vec![
            20, 30, 40, 50, 60, // tick 0
            120, 150, // tick 1
            220, 230, 240, 250, 260, 270, // tick 2
            350, // tick 4
            450, 460, 470, // tick 5
        ];
        let t0 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let stream = stream! {
            for i in 0..delays.len() {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();

                let delay = t0 + delays[i] - now;
                sleep(Duration::from_millis(delay as u64)).await;
                yield delays[i];
            }
        }
        .boxed();

        assert_eq!(
            vec![
                vec![20, 30, 40, 50, 60],           // tick 0
                vec![120, 150],                     // tick 1
                vec![220, 230, 240, 250, 260, 270], // tick 2
                vec![350],                          // tick 4
                vec![450, 460, 470],                // tick 5
                vec![],                             // keeps emitting empty windows
            ],
            stream
                .periodic_window(clock_stream)
                .collect::<Vec<_>>()
                .await
        );
    }

    /// Tests both the `periodic_window` and the fact that the `stream` is bounded and `clock_stream` unbounded
    /// and yet the `periodic_window` ends with the last window emit.
    #[tokio::test]
    async fn test_periodic_window_bounded() {
        let clock_freq = Duration::from_millis(100);
        let start = Instant::now() + clock_freq;
        // note, unbounded clock_stream, ie. unrestricted by input stream size
        let clock_stream = futures::stream::unfold(
            (tokio::time::interval_at(start, clock_freq), 0),
            |(mut interval, cnt)| async move {
                interval.tick().await;
                Some((cnt, (interval, cnt + 1)))
            },
        )
        .boxed(); // note, unbounded `clock_stream`

        // delays grouped per 100ms tick
        let delays = vec![
            20, 30, 40, 50, 60, // tick 0
            120, 150, // tick 1
            220, 230, 240, 250, 260, 270, // tick 2
            350, // tick 4
            450, 460, 470, // tick 5
            550, // tick 6 - note, stream ends prior to emit, won't see this window
        ];
        let t0 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let stream = stream! {
            for i in 0..delays.len() {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();

                let delay = t0 + delays[i] - now;
                sleep(Duration::from_millis(delay as u64)).await;
                yield delays[i];
            }
        }
        .boxed();

        assert_eq!(
            vec![
                vec![20, 30, 40, 50, 60],           // tick 0
                vec![120, 150],                     // tick 1
                vec![220, 230, 240, 250, 260, 270], // tick 2
                vec![350],                          // tick 4
                vec![450, 460, 470],                // tick 5
                                                    // note: tick 6 not emit!
            ],
            stream
                .periodic_window_bounded(clock_stream)
                .collect::<Vec<_>>()
                .await
        );
    }
}
