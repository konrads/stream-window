use futures::StreamExt;
use pin_project_lite::pin_project;
use std::pin::Pin;
use tokio_stream::Stream;

pub enum Either<T> {
    Entry(T),
    ClockTick,
}

pin_project! {
    pub struct PeriodicWindow<T> {
        stream: Pin<Box<dyn Stream<Item = Either<T>>>>,
        buffer: Vec<T>,
    }
}

// FIXME: enforce the merge to exit if any stream closes

impl<T> PeriodicWindow<T> {
    pub fn new<CT>(
        stream: impl Stream<Item = T> + Send + 'static,
        clock_stream: impl Stream<Item = CT> + Send + 'static,
    ) -> Self {
        let stream = stream.map(|x| Either::Entry(x));
        let clock_stream = clock_stream.map(|_| Either::<T>::ClockTick);
        let either_stream: Pin<Box<dyn Stream<Item = Either<T>>>> =
            tokio_stream::StreamExt::merge(stream, clock_stream).boxed();

        Self {
            stream: either_stream,
            buffer: vec![],
        }
    }
}

impl<T: Clone> Stream for PeriodicWindow<T> {
    type Item = Vec<T>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            std::task::Poll::Ready(Some(Either::Entry(element))) => {
                self.buffer.push(element);
                cx.waker().clone().wake();
                std::task::Poll::Pending
            }
            std::task::Poll::Ready(Some(Either::ClockTick)) => {
                std::task::Poll::Ready(Some(self.buffer.drain(..).collect()))
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}
