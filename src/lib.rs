mod sliding;
mod tumbling;

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
}

impl<S> WindowExt for S where S: Stream {}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;

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
}
