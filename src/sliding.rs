use pin_project_lite::pin_project;
use std::pin::Pin;
use tokio_stream::Stream;

pin_project! {
    /// A stream combinator which chunks up items of the stream into a sliding window of a given size.
    pub struct SlidingWindow<T, S>
    where
        S: Stream<Item = T>,
    {
        buffer: Vec<T>,
        window_size: usize,
        stream: S,
    }
}

impl<T, S> SlidingWindow<T, S>
where
    S: Stream<Item = T>,
{
    pub fn new(window_size: usize, stream: S) -> Self {
        assert!(window_size > 0, "Window size must be > 0");
        Self {
            buffer: vec![],
            window_size,
            stream,
        }
    }
}

impl<T, S> Stream for SlidingWindow<T, S>
where
    T: Clone,
    S: Stream<Item = T> + std::marker::Unpin,
{
    type Item = Vec<T>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            std::task::Poll::Ready(Some(element)) => {
                self.buffer.push(element);

                if self.buffer.len() > self.window_size {
                    // slide down
                    self.buffer.remove(0);
                }
                if self.buffer.len() == self.window_size {
                    std::task::Poll::Ready(Some(self.buffer.clone()))
                } else {
                    cx.waker().clone().wake();
                    std::task::Poll::Pending
                }
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => {
                cx.waker().clone().wake();
                std::task::Poll::Pending
            }
        }
    }
}
