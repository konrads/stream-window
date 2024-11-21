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

pub mod merge;

/// Tumbling window, waits till window of specified size is filled, emits, and starts a new window.
pub fn to_tumbling_window<T, InStream>(
    mut stream: InStream,
    window_size: usize,
) -> impl Stream<Item = Vec<T>>
where
    InStream: Stream<Item = T> + Unpin,
{
    let mut buffer = Vec::with_capacity(window_size);
    stream! {
        while let Some(element) = stream.next().await {
            buffer.push(element);
            if buffer.len() == window_size {
                yield buffer;
                buffer = Vec::with_capacity(window_size);
            }
        }
    }
}

/// Sliding window, waits till window of specified size is filled, emits, and slides down by one element.
pub fn to_sliding_window<T, InStream>(
    mut stream: InStream,
    window_size: usize,
) -> impl Stream<Item = Vec<T>>
where
    T: Clone,
    InStream: Stream<Item = T> + Unpin,
{
    let mut buffer = Vec::with_capacity(window_size + 1);
    stream! {
        while let Some(element) = stream.next().await {
            buffer.push(element);
            if buffer.len() == window_size {
                yield buffer.clone();
                buffer = buffer.split_off(1);
            }
        }
    }
}

/// Periodic window, waits till window of specified size is filled, emits, and starts a new window on a clock tick.
pub fn to_periodic_window<'a, T, CT, CTStream, InStream>(
    mut stream: InStream,
    mut clock_stream: CTStream,
    emit_last: bool,
) -> impl Stream<Item = Vec<T>>
where
    T: 'a,
    CTStream: Stream<Item = CT> + Unpin + 'a,
    InStream: Stream<Item = T> + Unpin + 'a,
{
    let mut buffer = vec![];
    stream! {
        loop {
            select! {
                biased;

                _ = clock_stream.next() => {
                    yield buffer;
                    buffer = vec![];
                }

                element = stream.next() => {
                    let Some(element) = element else {
                        if emit_last {
                            yield buffer;
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
pub trait WindowExt: Stream + Sized + Send
where
    Self::Item: Send,
{
    fn tumbling_window<'a>(
        self,
        window_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Vec<Self::Item>> + 'a>>
    where
        Self: Unpin + 'a,
    {
        to_tumbling_window(self, window_size).boxed()
    }

    fn tumbling_window_unpin<'a>(
        self,
        window_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Vec<Self::Item>> + 'a>>
    where
        Self: 'a,
    {
        to_tumbling_window(Box::pin(self), window_size).boxed()
    }

    fn sliding_window<'a>(
        self,
        window_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Vec<Self::Item>> + 'a>>
    where
        Self: Unpin + 'a,
        Self::Item: Clone,
    {
        to_sliding_window(self, window_size).boxed()
    }

    fn sliding_window_unpin<'a>(
        self,
        window_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Vec<Self::Item>> + 'a>>
    where
        Self: 'a,
        Self::Item: Clone,
    {
        to_sliding_window(Box::pin(self), window_size).boxed()
    }

    fn periodic_window<'a, CT, CTStream>(
        self,
        clock_stream: CTStream,
        emit_last: bool,
    ) -> Pin<Box<dyn Stream<Item = Vec<Self::Item>> + 'a>>
    where
        CT: Send + 'a,
        Self: Unpin + 'a,
        CTStream: Stream<Item = CT> + Unpin + Send + 'a,
    {
        to_periodic_window(self, clock_stream, emit_last).boxed()
    }

    fn periodic_window_unpin<'a, CT, CTStream>(
        self,
        clock_stream: CTStream,
        emit_last: bool,
    ) -> Pin<Box<dyn Stream<Item = Vec<Self::Item>> + 'a>>
    where
        CT: Send + 'a,
        Self: 'a,
        CTStream: Stream<Item = CT> + Send + 'a,
    {
        to_periodic_window(Box::pin(self), Box::pin(clock_stream), emit_last).boxed()
    }
}

impl<S> WindowExt for S
where
    S: Stream + Sized + Send,
    S::Item: Send,
{
}

#[cfg(test)]
mod tests;
