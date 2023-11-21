use async_stream::stream;
use futures::StreamExt;
use tokio::select;
use tokio_stream::Stream;

pub fn to_sliding_window<'a, T: Clone + 'a>(
    mut stream: impl Stream<Item = T> + Unpin + 'a,
    window_size: usize,
) -> impl Stream<Item = Vec<T>> + 'a {
    let mut buffer = Vec::with_capacity(window_size);
    stream! {
        loop {
            select! {
                element = stream.next() => {
                    let Some(element) = element else { break };

                    buffer.push(element);
                    if buffer.len() == window_size {
                        yield buffer.clone();
                        // slide down
                        buffer.remove(0);
                    }
                }
            }
        }
    }
}
