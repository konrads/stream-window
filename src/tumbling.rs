use async_stream::stream;
use futures::StreamExt;
use tokio_stream::Stream;

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
