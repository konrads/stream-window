use pin_project_lite::pin_project;
use std::pin::Pin;
use tokio_stream::Stream;

pin_project! {
    /// A stream combinator which chunks up items of the stream into a tumbling window of a given size.
    pub struct TumblingWindow<T, S>
    where
        S: Stream<Item = T>,
    {
        buffer: Vec<T>,
        window_size: usize,
        stream: S,
    }
}

impl<T, S> TumblingWindow<T, S>
where
    S: Stream<Item = T>,
{
    pub fn new(window_size: usize, stream: S) -> Self {
        assert!(window_size > 0, "Window size must be > 0");
        Self {
            buffer: Vec::new(),
            window_size,
            stream,
        }
    }
}

impl<T, S> Stream for TumblingWindow<T, S>
where
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

                if self.buffer.len() >= self.window_size {
                    // The window size has been reached, emit the window and clear the buffer.
                    let window = std::mem::take(&mut self.buffer);
                    std::task::Poll::Ready(Some(window))
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
