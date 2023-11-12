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

    fn periodic_window<CT>(
        self,
        clock_stream: impl Stream<Item = CT> + Send + 'static,
    ) -> PeriodicWindow<Self::Item>
    where
        Self: Send + Sized + 'static,
    {
        PeriodicWindow::new(self, clock_stream)
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

    #[tokio::test]
    async fn test_periodic_window() {
        let clock_freq = Duration::from_millis(100);
        let start = Instant::now() + clock_freq;
        let clock_stream = futures::stream::unfold(
            (tokio::time::interval_at(start, clock_freq), 0),
            |(mut interval, cnt)| async move {
                interval.tick().await;
                Some((cnt, (interval, cnt + 1)))
            },
        )
        .take(5)
        .boxed();

        // delays grouped per 100ms tick
        let delays = vec![
            vec![20, 30, 40, 50, 60],           // tick 0
            vec![120, 150],                     // tick 1
            vec![220, 230, 240, 250, 260, 270], // tick 2
            vec![350],                          // tick 4
            vec![450, 460, 470],                // tick 5
        ];
        let delays_flatten = delays.iter().flatten().cloned().collect::<Vec<_>>();
        let t0 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let stream = stream! {
            for i in 0..delays_flatten.len() {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();

                let delay = t0 + delays_flatten[i] - now;
                sleep(Duration::from_millis(delay as u64)).await;
                yield delays_flatten[i];
            }
        }
        .boxed();

        let exp_window_vals = delays; // expect just the emitted delays
        let periodic_window = stream.periodic_window(clock_stream);
        assert_eq!(exp_window_vals, periodic_window.collect::<Vec<_>>().await);
    }

    #[tokio::test]
    async fn test_periodic_window_ends_when_any_stream_ends() {
        unimplemented!()
    }
}
