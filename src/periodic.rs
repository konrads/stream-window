use async_stream::stream;
use futures::StreamExt;
use tokio::select;
use tokio_stream::Stream;

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
