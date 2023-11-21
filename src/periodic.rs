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
impl<T: 'static> PeriodicWindow<T> {
    pub fn new<CT>(
        stream: impl Stream<Item = T> + Send + 'static,
        clock_stream: impl Stream<Item = CT> + Send + 'static,
    ) -> Self {
        let stream = stream.map(|x| Some(Either::Entry(x)));
        let clock_stream = clock_stream.map(|_| Some(Either::<T>::ClockTick));

        // map to Option<Either>
        let stream_terminated = stream.chain(futures::stream::once(future::ready(None)));
        let clock_stream_terminated =
            clock_stream.chain(futures::stream::once(future::ready(None)));

        // merge & take while not None
        let merged_stream: Pin<Box<dyn Stream<Item = Either<T>>>> =
            tokio_stream::StreamExt::map_while(
                tokio_stream::StreamExt::merge(stream_terminated, clock_stream_terminated),
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
            std::task::Poll::Ready(None)
        } else {
            match self.stream.poll_next_unpin(cx) {
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
    mut stream: impl Stream<Item = T> + Unpin + 'a,
    mut clock_stream: impl Stream<Item = CT> + Unpin + 'a,
    emit_last: bool,
) -> impl Stream<Item = Vec<T>> + 'a {
    let mut buffer = vec![];
    stream! {
        loop {
            select! {
                biased;

                _ = clock_stream.next() => {
                    yield std::mem::take(&mut buffer)
                }
                element = stream.next() => {
                    if let Some(element) = element {
                        buffer.push(element);
                    } else {
                        if emit_last {
                            yield std::mem::take(&mut buffer)
                        }
                        break;
                    }
                }
            }
        }
    }
}
