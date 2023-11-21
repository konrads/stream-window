use async_stream::stream;
use futures::StreamExt;
use pin_project_lite::pin_project;
use std::{convert, future, pin::Pin};
use tokio::select;
use tokio_stream::Stream;

pub enum Either<T> {
    Entry(T),
    ClockTick,
}

pin_project! {
    pub struct PeriodicWindow<T> {
        stream: Pin<Box<dyn Stream<Item = Either<T>>>>,
        buffer: Vec<T>,
        encountered_last: bool,
    }
}

/// Window that merges `stream` and `clock_stream`, ensuring termination
/// of the resulting stream should the either of the merged streams terminates.
impl<T> PeriodicWindow<T> {
    pub fn new<CT>(
        stream: impl Stream<Item = T> + Send + 'static,
        clock_stream: impl Stream<Item = CT> + Send + 'static,
    ) -> Self
    where
        T: 'static,
    {
        let stream = stream.map(|x| Either::Entry(x));
        let clock_stream = clock_stream.map(|_| Either::<T>::ClockTick);

        // map to Option<Either>
        let stream_opt = stream
            .map(Some)
            .chain(futures::stream::once(future::ready(None)));
        let clock_stream_opt = clock_stream
            .map(Some)
            .chain(futures::stream::once(future::ready(None)));

        // merge & take while not None
        let merged_stream: Pin<Box<dyn Stream<Item = Either<T>>>> =
            tokio_stream::StreamExt::map_while(
                tokio_stream::StreamExt::merge(stream_opt, clock_stream_opt),
                convert::identity::<Option<Either<T>>>,
            )
            .boxed_local();

        Self {
            stream: merged_stream,
            buffer: vec![],
            encountered_last: false,
        }
    }
}

impl<T: Clone> Stream for PeriodicWindow<T> {
    type Item = Vec<T>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.encountered_last {
            return std::task::Poll::Ready(None);
        } else {
            match Pin::new(&mut self.stream).poll_next(cx) {
                std::task::Poll::Ready(Some(Either::Entry(element))) => {
                    self.buffer.push(element);
                    cx.waker().clone().wake();
                    std::task::Poll::Pending
                }
                std::task::Poll::Ready(Some(Either::ClockTick)) => {
                    std::task::Poll::Ready(Some(self.buffer.drain(..).collect()))
                }
                std::task::Poll::Ready(None) => {
                    self.encountered_last = true;
                    std::task::Poll::Ready(Some(self.buffer.drain(..).collect()))
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        }
    }
}

/// Implementation using Unpins and mutable refs
#[allow(dead_code)]
pub fn to_periodic_window<'a, T: 'a, CT>(
    stream: &'a mut (impl Stream<Item = T> + Unpin),
    clock_stream: &'a mut (impl Stream<Item = CT> + Unpin),
    emit_last: bool,
) -> impl Stream<Item = Vec<T>> + 'a {
    let mut buffer = vec![];
    stream! {
        loop {
            select! {
                biased;

                _ = clock_stream.next() => {
                    yield buffer.drain(..).collect::<Vec<_>>()
                }
                element = stream.next() => {
                    if let Some(element) = element {
                        buffer.push(element);
                    } else {
                        if emit_last {
                            yield buffer.drain(..).collect::<Vec<_>>();
                        }
                        break;
                    }
                }
            }
        }
    }
}
