//! Stream windows build on top of async streams.
//! Implementations offered:
//! - tumbling window, waits till window of specified size is filled, emits, and starts a new window
//! - sliding window, waits till window of specified size is filled, emits, and slides down by one element
//! - periodic window, waits till window of specified size is filled, emits, and starts a new window on a clock tick

use async_stream::stream;
use futures::StreamExt;
use std::pin::Pin;
use tokio::select;
use tokio_stream::Stream;

/// Tumbling window, waits till window of specified size is filled, emits, and starts a new window.
pub fn to_tumbling_window<'a, T: Clone + 'a>(
    mut stream: impl Stream<Item = T> + Unpin + 'a,
    window_size: usize,
) -> impl Stream<Item = Vec<T>> + 'a {
    let mut buffer = Vec::with_capacity(window_size);
    stream! {
        while let Some(element) = stream.next().await {
            buffer.push(element);
            if buffer.len() == window_size {
                yield core::mem::take(&mut buffer);
            }
        }
    }
}

/// Sliding window, waits till window of specified size is filled, emits, and slides down by one element.
pub fn to_sliding_window<'a, T: Clone + 'a>(
    mut stream: impl Stream<Item = T> + Unpin + 'a,
    window_size: usize,
) -> impl Stream<Item = Vec<T>> + 'a {
    let mut buffer = Vec::with_capacity(window_size);
    stream! {
        while let Some(element) = stream.next().await {
            buffer.push(element);
            if buffer.len() == window_size {
                yield buffer.clone();
                // slide down
                buffer.remove(0);
            }
        }
    }
}

/// Periodic window,
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
                    let Some(element) = element else {
                        if emit_last {
                            yield std::mem::take(&mut buffer)
                        }
                        break;
                    };

                    buffer.push(element);
                }
            }
        }
    }
}

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
mod tests;
