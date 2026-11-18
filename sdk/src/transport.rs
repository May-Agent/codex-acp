//! Cancel-safe LF-only (`\n`) newline-delimited JSON duplex transport.
//!
//! `codex app-server` frames messages strictly on ASCII LF (`0x0A`). JSON string
//! payloads may contain unescaped Unicode line and paragraph separators
//! (`U+2028` and `U+2029`), so framing never splits or trims on Unicode line
//! breaks.
//!
//! [`NdjsonReader::next_line`] buffers partial frames inside the reader so
//! dropping an in-flight read future inside `or` or `select` never discards
//! bytes already read from the underlying stream.

use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll};

use codex_codes::JsonRpcMessage;
use futures_lite::io::{AsyncBufRead, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::trace;

use crate::error::{Result, SdkError};

/// Read buffer capacity (64 KiB) before per-frame dynamic growth.
const READ_BUF_CAPACITY: usize = 64 * 1024;

/// Write buffer capacity (16 KiB) flushed once per outbound NDJSON frame.
const WRITE_BUF_CAPACITY: usize = 16 * 1024;

/// Cancel-safe LF-delimited NDJSON frame reader over any [`AsyncRead`].
pub struct NdjsonReader<R> {
    reader: BufReader<R>,
    buf: Vec<u8>,
}

impl<R> fmt::Debug for NdjsonReader<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NdjsonReader")
            .field("buffered_bytes", &self.buf.len())
            .finish_non_exhaustive()
    }
}

impl<R: AsyncRead + Unpin + Send> NdjsonReader<R> {
    /// Wrap an [`AsyncRead`] stream in a buffered LF-only NDJSON reader.
    #[must_use]
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::with_capacity(READ_BUF_CAPACITY, reader),
            buf: Vec::new(),
        }
    }

    /// Read the next non-blank LF-delimited UTF-8 line, or `Ok(None)` at EOF.
    ///
    /// Only ASCII LF (`\n`) terminates a frame (with an optional preceding `\r`
    /// stripped). Unescaped `U+2028` and `U+2029` characters are preserved
    /// intact. Dropping the returned future before completion retains any bytes
    /// already buffered inside `self`.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Io`] on underlying read errors or
    /// [`SdkError::Protocol`] if a frame is not valid UTF-8.
    pub async fn next_line(&mut self) -> Result<Option<String>> {
        poll_fn(|cx| self.poll_next_line(cx)).await
    }

    /// Read and deserialize the next JSON frame of type `T`, or `Ok(None)` at EOF.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Io`] on read failure, [`SdkError::Protocol`] on
    /// invalid UTF-8, or [`SdkError::Json`] if JSON deserialization fails.
    pub async fn recv_json<T: DeserializeOwned>(&mut self) -> Result<Option<T>> {
        let Some(line) = self.next_line().await? else {
            return Ok(None);
        };
        trace!(bytes = line.len(), "received NDJSON frame");
        let parsed = serde_json::from_str(&line)?;
        Ok(Some(parsed))
    }

    /// Read and decode the next [`JsonRpcMessage`] frame from the stream, or
    /// `Ok(None)` at EOF.
    ///
    /// # Errors
    ///
    /// Propagates errors from [`Self::recv_json`].
    pub async fn recv_message(&mut self) -> Result<Option<JsonRpcMessage>> {
        self.recv_json().await
    }

    fn poll_next_line(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<String>>> {
        loop {
            let available = match Pin::new(&mut self.reader).poll_fill_buf(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(SdkError::Io(err))),
                Poll::Ready(Ok(buf)) => buf,
            };

            if available.is_empty() {
                if self.buf.is_empty() {
                    return Poll::Ready(Ok(None));
                }
                let raw = std::mem::take(&mut self.buf);
                return Poll::Ready(finish_frame(raw));
            }

            if let Some(newline_pos) = available.iter().position(|&b| b == b'\n') {
                self.buf.extend_from_slice(&available[..newline_pos]);
                Pin::new(&mut self.reader).consume(newline_pos + 1);
                let raw = std::mem::take(&mut self.buf);
                match finish_frame(raw) {
                    Ok(Some(line)) => return Poll::Ready(Ok(Some(line))),
                    Ok(None) => continue,
                    Err(err) => return Poll::Ready(Err(err)),
                }
            }

            let len = available.len();
            self.buf.extend_from_slice(available);
            Pin::new(&mut self.reader).consume(len);
        }
    }
}

fn finish_frame(mut raw: Vec<u8>) -> Result<Option<String>> {
    if raw.last() == Some(&b'\r') {
        raw.pop();
    }
    if raw.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    String::from_utf8(raw)
        .map(Some)
        .map_err(|err| SdkError::Protocol(format!("NDJSON frame is not valid UTF-8: {err}")))
}

/// LF-delimited NDJSON frame writer over any [`AsyncWrite`].
pub struct NdjsonWriter<W> {
    writer: BufWriter<W>,
}

impl<W> fmt::Debug for NdjsonWriter<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NdjsonWriter").finish_non_exhaustive()
    }
}

impl<W: AsyncWrite + Unpin + Send> NdjsonWriter<W> {
    /// Wrap an [`AsyncWrite`] sink in a buffered LF-only NDJSON writer.
    #[must_use]
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::with_capacity(WRITE_BUF_CAPACITY, writer),
        }
    }

    /// Write one pre-serialized JSON frame terminated by `\n` and flush.
    ///
    /// Rejects embedded `\r` or `\n` bytes while preserving unescaped `U+2028`
    /// and `U+2029` characters verbatim.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Protocol`] if `line` contains a literal `\r` or `\n`,
    /// or [`SdkError::Io`] if writing or flushing fails.
    pub async fn write_raw_line(&mut self, line: &str) -> Result<()> {
        if line.as_bytes().iter().any(|&b| b == b'\n' || b == b'\r') {
            return Err(SdkError::Protocol(
                "outbound NDJSON frame contains embedded CR or LF".to_string(),
            ));
        }
        trace!(bytes = line.len(), "sending NDJSON frame");
        self.writer.write_all(line.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Serialize `value` as single-line JSON (preserving unescaped `U+2028` and
    /// `U+2029`), append `\n`, and flush.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Json`] if serialization fails or [`SdkError::Io`] if
    /// writing to the underlying sink fails.
    pub fn send_json<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> impl Future<Output = Result<()>> + Send + use<'_, W, T> {
        let serialized = serde_json::to_string(value).map_err(SdkError::Json);
        async move { self.write_raw_line(&serialized?).await }
    }

    /// Flush and close the underlying [`AsyncWrite`] sink.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Io`] if flushing or closing fails.
    pub async fn close(&mut self) -> Result<()> {
        self.writer.flush().await?;
        self.writer.close().await?;
        Ok(())
    }
}

/// Paired LF-only NDJSON duplex transport over [`AsyncRead`] and [`AsyncWrite`].
#[derive(Debug)]
pub struct NdjsonTransport<R, W> {
    reader: NdjsonReader<R>,
    writer: NdjsonWriter<W>,
}

impl<R, W> NdjsonTransport<R, W>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    /// Create a duplex NDJSON transport from separate read and write halves.
    #[must_use]
    pub fn new(read: R, write: W) -> Self {
        Self {
            reader: NdjsonReader::new(read),
            writer: NdjsonWriter::new(write),
        }
    }

    /// Split into independent [`NdjsonReader`] and [`NdjsonWriter`] halves.
    #[must_use]
    pub fn split(self) -> (NdjsonReader<R>, NdjsonWriter<W>) {
        (self.reader, self.writer)
    }

    /// Mutable reference to the inbound [`NdjsonReader`].
    pub const fn reader_mut(&mut self) -> &mut NdjsonReader<R> {
        &mut self.reader
    }

    /// Mutable reference to the outbound [`NdjsonWriter`].
    pub const fn writer_mut(&mut self) -> &mut NdjsonWriter<W> {
        &mut self.writer
    }

    /// Read the next LF-delimited line from the inbound half.
    ///
    /// # Errors
    ///
    /// Propagates errors from [`NdjsonReader::next_line`].
    pub async fn next_line(&mut self) -> Result<Option<String>> {
        self.reader.next_line().await
    }

    /// Read and deserialize the next JSON frame from the inbound half.
    ///
    /// # Errors
    ///
    /// Propagates errors from [`NdjsonReader::recv_json`].
    pub async fn recv_json<T: DeserializeOwned>(&mut self) -> Result<Option<T>> {
        self.reader.recv_json().await
    }

    /// Write a raw single-line JSON string on the outbound half.
    ///
    /// # Errors
    ///
    /// Propagates errors from [`NdjsonWriter::write_raw_line`].
    pub async fn write_raw_line(&mut self, line: &str) -> Result<()> {
        self.writer.write_raw_line(line).await
    }

    /// Serialize and send `value` on the outbound half.
    ///
    /// # Errors
    ///
    /// Propagates errors from [`NdjsonWriter::send_json`].
    pub fn send_json<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> impl Future<Output = Result<()>> + Send + use<'_, R, W, T> {
        self.writer.send_json(value)
    }

    /// Flush and close the outbound half.
    ///
    /// # Errors
    ///
    /// Propagates errors from [`NdjsonWriter::close`].
    pub async fn close(&mut self) -> Result<()> {
        self.writer.close().await
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use futures_lite::stream::Stream;
    use serde_json::{Value, json};

    use super::*;

    struct MemoryPipe {
        rx: Pin<Box<async_channel::Receiver<Vec<u8>>>>,
        tx: async_channel::Sender<Vec<u8>>,
        chunk: Vec<u8>,
        pos: usize,
    }

    fn pipe_pair() -> (MemoryPipe, MemoryPipe) {
        let (tx_a, rx_b) = async_channel::unbounded();
        let (tx_b, rx_a) = async_channel::unbounded();
        (
            MemoryPipe {
                rx: Box::pin(rx_a),
                tx: tx_a,
                chunk: Vec::new(),
                pos: 0,
            },
            MemoryPipe {
                rx: Box::pin(rx_b),
                tx: tx_b,
                chunk: Vec::new(),
                pos: 0,
            },
        )
    }

    impl AsyncRead for MemoryPipe {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = &mut *self;
            if this.pos < this.chunk.len() {
                let rem = &this.chunk[this.pos..];
                let n = rem.len().min(buf.len());
                buf[..n].copy_from_slice(&rem[..n]);
                this.pos += n;
                if this.pos == this.chunk.len() {
                    this.chunk.clear();
                    this.pos = 0;
                }
                return Poll::Ready(Ok(n));
            }
            match this.rx.as_mut().poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(Ok(0)),
                Poll::Ready(Some(data)) => {
                    let n = data.len().min(buf.len());
                    buf[..n].copy_from_slice(&data[..n]);
                    if n < data.len() {
                        this.chunk = data;
                        this.pos = n;
                    }
                    Poll::Ready(Ok(n))
                }
            }
        }
    }

    impl AsyncWrite for MemoryPipe {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if buf.is_empty() {
                return Poll::Ready(Ok(0));
            }
            self.tx
                .try_send(buf.to_vec())
                .map(|()| buf.len())
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "closed"))
                .into()
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.tx.close();
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn preserves_u2028_and_u2029_and_strips_crlf() {
        let (mut a, b) = pipe_pair();
        let mut transport = NdjsonTransport::new(
            b,
            MemoryPipe {
                rx: Box::pin(async_channel::unbounded().1),
                tx: async_channel::unbounded().0,
                chunk: Vec::new(),
                pos: 0,
            },
        );

        a.write_all("{\"text\":\"x\u{2028}y\u{2029}z\"}\r\n".as_bytes())
            .await
            .unwrap();
        let line = transport.next_line().await.unwrap().unwrap();
        assert!(line.contains('\u{2028}'));
        assert!(line.contains('\u{2029}'));
        assert!(!line.ends_with('\r'));
        let decoded: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(decoded["text"], "x\u{2028}y\u{2029}z");
    }

    #[tokio::test]
    async fn retains_buffered_partial_frame_across_cancelled_read() {
        let (mut writer, reader) = pipe_pair();
        let mut ndjson = NdjsonReader::new(reader);

        writer.write_all(br#"{"id":7,"method":"#).await.unwrap();
        {
            let mut read_fut = pin!(ndjson.next_line());
            let mut cx = Context::from_waker(Waker::noop());
            assert!(matches!(read_fut.as_mut().poll(&mut cx), Poll::Pending));
        }
        writer
            .write_all(b"\"ping\",\"params\":{\"ok\":true}}\n")
            .await
            .unwrap();

        let msg: Value = ndjson.recv_json().await.unwrap().unwrap();
        assert_eq!(
            msg,
            json!({"id": 7, "method": "ping", "params": {"ok": true}})
        );
    }

    #[tokio::test]
    async fn rejects_embedded_cr_and_lf_on_write() {
        let (a, _b) = pipe_pair();
        let mut writer = NdjsonWriter::new(a);
        assert!(matches!(
            writer.write_raw_line("{\"a\":1}\n{\"b\":2}").await,
            Err(SdkError::Protocol(_))
        ));
        assert!(matches!(
            writer.write_raw_line("{\"a\":1}\r{\"b\":2}").await,
            Err(SdkError::Protocol(_))
        ));
    }
}
