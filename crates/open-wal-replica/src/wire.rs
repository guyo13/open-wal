//! Wire protocol codec (§6 of `docs/replica_design_v1.md`).
//!
//! Every frame is `len:u32 | type:u8 | body`, all integers little-endian. `len`
//! counts the bytes **after** the length prefix (the type byte plus the body), so
//! a well-formed frame occupies `4 + len` bytes and `len ≥ 1`.
//!
//! | type | frame | body |
//! |---|---|---|
//! | 1 | `HELLO` | `proto_ver:u16, replica_id:u64, durable_lsn:u64` |
//! | 2 | `SERVING` | `from_lsn:u64, primary_oldest:u64` |
//! | 3 | `RESEED_REQUIRED` | `primary_oldest:u64` |
//! | 4 | `RECORD` | `lsn:u64, crc:u32, payload:[u8]` — `crc = crc32c(lsn_le ‖ payload)` |
//! | 5 | `ACK` | `durable_lsn:u64` |
//! | 6 | `HEARTBEAT` | `durable_lsn:u64` |
//! | 7 | `ERR` | `code:u16, msg:[u8]` (UTF-8, ≤ [`MAX_ERR_MSG`] bytes) |
//!
//! **The decoder is attacker-facing.** [`decode_frame`] is a pure function of
//! its input bytes: it never panics, never reads out of bounds, and never
//! allocates. A frame whose declared `len` exceeds the configured [`Limits`] is
//! rejected from its 4-byte prefix alone — before any body byte is buffered — so
//! a hostile peer cannot make the streaming [`FrameReader`] grow without bound.
//! Unknown types, wrong fixed-body sizes, oversize payloads, unknown `ERR` codes
//! and non-UTF-8 `ERR` messages are all [`ReplError::Protocol`]. A `RECORD`
//! whose CRC does not match is [`ReplError::WireCrc`] (R9).

use std::io::{self, Read};

use open_wal::Lsn;

use crate::error::{ReplError, Result};

/// Protocol version carried in `HELLO` (§6). A peer speaking another version is
/// answered with `ERR(Protocol)`.
pub const PROTO_VERSION: u16 = 1;

/// Bytes in the `len:u32` frame prefix.
pub const LEN_PREFIX: usize = 4;

/// Upper bound on an `ERR` message, in bytes. Longer messages are truncated (at
/// a UTF-8 boundary) on encode and rejected on decode.
pub const MAX_ERR_MSG: usize = 1024;

/// Fixed part of a `RECORD` body: `lsn:u64` + `crc:u32`.
pub const RECORD_FIXED: usize = 12;

const T_HELLO: u8 = 1;
const T_SERVING: u8 = 2;
const T_RESEED: u8 = 3;
const T_RECORD: u8 = 4;
const T_ACK: u8 = 5;
const T_HEARTBEAT: u8 = 6;
const T_ERR: u8 = 7;

const HELLO_BODY: usize = 2 + 8 + 8;
const SERVING_BODY: usize = 8 + 8;
const LSN_BODY: usize = 8;

/// The error classes an `ERR` frame carries (§6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ErrCode {
    /// R3 stream-contiguity violation (or a `HELLO` the primary must refuse,
    /// §9 step 2).
    Contiguity = 1,
    /// R9 `RECORD` CRC mismatch.
    WireCrc = 2,
    /// Malformed / unknown / out-of-sequence frame, or version mismatch.
    Protocol = 3,
    /// The sender's WAL is poisoned (WAL §12); it must reopen before resuming.
    Poisoned = 4,
}

impl ErrCode {
    fn from_u16(v: u16) -> Option<ErrCode> {
        match v {
            1 => Some(ErrCode::Contiguity),
            2 => Some(ErrCode::WireCrc),
            3 => Some(ErrCode::Protocol),
            4 => Some(ErrCode::Poisoned),
            _ => None,
        }
    }
}

/// One decoded (or to-be-encoded) frame. Variable-length bodies borrow from the
/// buffer they were decoded from, so decoding never allocates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Frame<'a> {
    /// R→P: "I am durable to `durable_lsn`; serve me from `+1`."
    Hello {
        /// Must equal [`PROTO_VERSION`] to be served.
        proto_ver: u16,
        /// The replica's self-declared identity (informational).
        replica_id: u64,
        /// The replica's `durable_lsn` (its resume point, R7).
        durable_lsn: Lsn,
    },
    /// P→R: the primary will stream from `from_lsn` (= the replica's
    /// `durable_lsn + 1`).
    Serving {
        /// First LSN the primary will send.
        from_lsn: Lsn,
        /// The primary's oldest retained LSN (`Lsn(0)` = not reported).
        primary_oldest: Lsn,
    },
    /// P→R: the replica is behind the primary's retention floor (R6).
    ReseedRequired {
        /// The primary's oldest retained LSN.
        primary_oldest: Lsn,
    },
    /// P→R: one record. Encoding computes the CRC; decoding verifies it (a
    /// mismatch is [`ReplError::WireCrc`]), so a decoded `Record` is
    /// byte-faithful to what the sender framed.
    Record {
        /// The record's LSN (identical on primary and replica, R2).
        lsn: Lsn,
        /// The opaque payload.
        payload: &'a [u8],
    },
    /// R→P: the replica's durable watermark (R4). Monotonic.
    Ack {
        /// The replica's `durable_lsn` after a successful `commit`.
        durable_lsn: Lsn,
    },
    /// Both directions: keepalive carrying the sender's watermark.
    Heartbeat {
        /// The sender's watermark (`durable_lsn` on a replica, the released
        /// watermark on the primary).
        durable_lsn: Lsn,
    },
    /// Both directions: an error; the sender closes after sending it.
    Err {
        /// The error class.
        code: ErrCode,
        /// A short UTF-8 message (≤ [`MAX_ERR_MSG`] bytes).
        msg: &'a str,
    },
}

/// Decoder bounds. The only knob is the largest `RECORD` payload the receiver
/// will accept — the WAL's `max_record_size` (which MUST match between primary
/// and replica, §7 step 1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Largest `RECORD` payload accepted, in bytes.
    pub max_payload: u32,
}

impl Limits {
    /// Limits for a stream carrying records of at most `max_record_size` bytes.
    #[must_use]
    pub const fn new(max_record_size: u32) -> Limits {
        Limits {
            max_payload: max_record_size,
        }
    }

    /// Largest acceptable `len` field (type byte + body). Computed in `u64` so a
    /// `max_payload` near `u32::MAX` cannot overflow.
    #[must_use]
    pub fn max_len(&self) -> u64 {
        let record = RECORD_FIXED as u64 + u64::from(self.max_payload);
        let err = 2 + MAX_ERR_MSG as u64;
        1 + record.max(err).max(HELLO_BODY as u64)
    }
}

/// Result of [`decode_frame`] on a buffer that holds a frame prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decoded<'a> {
    /// A complete, valid frame that occupied the first `consumed` bytes.
    Frame(Frame<'a>, usize),
    /// The buffer holds only part of a frame; `total` bytes are needed in all
    /// (`LEN_PREFIX` if even the length prefix is incomplete). `total` is always
    /// within [`Limits::max_len`]` + LEN_PREFIX`.
    Incomplete {
        /// Total bytes the first frame needs.
        total: usize,
    },
}

/// `crc32c(lsn_le ‖ payload)` — the `RECORD` checksum (§6, R9).
#[must_use]
pub fn record_crc(lsn: Lsn, payload: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&lsn.0.to_le_bytes()), payload)
}

/// The total encoded size (prefix included) of the first frame in `buf`, or
/// `None` if fewer than [`LEN_PREFIX`] bytes are present. Rejects a zero or
/// over-limit `len` from the prefix alone (no body is needed to reject it).
pub fn frame_len(buf: &[u8], limits: &Limits) -> Result<Option<usize>> {
    let Some(prefix) = buf.get(..LEN_PREFIX) else {
        return Ok(None);
    };
    let len = u32::from_le_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]);
    if len == 0 {
        return Err(ReplError::Protocol("zero-length frame"));
    }
    if u64::from(len) > limits.max_len() {
        return Err(ReplError::Protocol("frame exceeds length limit"));
    }
    // `len ≤ max_len ≤ 2^32 + 13`, which fits `usize` on every 64-bit target;
    // `try_from` keeps 32-bit targets bounds-safe too.
    let total = usize::try_from(u64::from(len) + LEN_PREFIX as u64)
        .map_err(|_| ReplError::Protocol("frame exceeds length limit"))?;
    Ok(Some(total))
}

fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([b[at], b[at + 1]])
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(a)
}

fn exact(body: &[u8], n: usize) -> Result<&[u8]> {
    if body.len() == n {
        Ok(body)
    } else {
        Err(ReplError::Protocol("wrong body size for frame type"))
    }
}

/// Decode the first frame in `buf`. Pure, allocation-free, bounds-safe for any
/// input (the property the RM8 decoder fuzz target asserts).
///
/// Returns [`Decoded::Incomplete`] when `buf` holds only a prefix of a frame;
/// the caller reads more bytes and retries. Trailing bytes after the first
/// frame are ignored (they belong to the next frame).
pub fn decode_frame<'a>(buf: &'a [u8], limits: &Limits) -> Result<Decoded<'a>> {
    let Some(total) = frame_len(buf, limits)? else {
        return Ok(Decoded::Incomplete { total: LEN_PREFIX });
    };
    let Some(frame) = buf.get(LEN_PREFIX..total) else {
        return Ok(Decoded::Incomplete { total });
    };
    // `len ≥ 1` (checked in frame_len), so the type byte exists.
    let (ty, body) = (frame[0], &frame[1..]);
    let f = match ty {
        T_HELLO => {
            let b = exact(body, HELLO_BODY)?;
            Frame::Hello {
                proto_ver: u16_at(b, 0),
                replica_id: u64_at(b, 2),
                durable_lsn: Lsn(u64_at(b, 10)),
            }
        }
        T_SERVING => {
            let b = exact(body, SERVING_BODY)?;
            Frame::Serving {
                from_lsn: Lsn(u64_at(b, 0)),
                primary_oldest: Lsn(u64_at(b, 8)),
            }
        }
        T_RESEED => Frame::ReseedRequired {
            primary_oldest: Lsn(u64_at(exact(body, LSN_BODY)?, 0)),
        },
        T_RECORD => {
            if body.len() < RECORD_FIXED {
                return Err(ReplError::Protocol("short RECORD body"));
            }
            let payload = &body[RECORD_FIXED..];
            if payload.len() as u64 > u64::from(limits.max_payload) {
                return Err(ReplError::Protocol(
                    "RECORD payload exceeds max_record_size",
                ));
            }
            let lsn = Lsn(u64_at(body, 0));
            if u32_at(body, 8) != record_crc(lsn, payload) {
                return Err(ReplError::WireCrc);
            }
            Frame::Record { lsn, payload }
        }
        T_ACK => Frame::Ack {
            durable_lsn: Lsn(u64_at(exact(body, LSN_BODY)?, 0)),
        },
        T_HEARTBEAT => Frame::Heartbeat {
            durable_lsn: Lsn(u64_at(exact(body, LSN_BODY)?, 0)),
        },
        T_ERR => {
            if body.len() < 2 {
                return Err(ReplError::Protocol("short ERR body"));
            }
            let code = ErrCode::from_u16(u16_at(body, 0))
                .ok_or(ReplError::Protocol("unknown ERR code"))?;
            let raw = &body[2..];
            if raw.len() > MAX_ERR_MSG {
                return Err(ReplError::Protocol("ERR message too long"));
            }
            let msg = std::str::from_utf8(raw)
                .map_err(|_| ReplError::Protocol("ERR message not UTF-8"))?;
            Frame::Err { code, msg }
        }
        _ => return Err(ReplError::Protocol("unknown frame type")),
    };
    Ok(Decoded::Frame(f, total))
}

fn put_header(out: &mut Vec<u8>, ty: u8, body_len: usize) {
    let len = u32::try_from(1 + body_len).expect("frame body exceeds u32 length field");
    out.extend_from_slice(&len.to_le_bytes());
    out.push(ty);
}

/// Append one `RECORD` frame for `(lsn, payload)` to `out`, computing its CRC.
/// The shipper's hot encode path (no intermediate [`Frame`]).
pub fn encode_record(out: &mut Vec<u8>, lsn: Lsn, payload: &[u8]) {
    put_header(out, T_RECORD, RECORD_FIXED + payload.len());
    out.extend_from_slice(&lsn.0.to_le_bytes());
    out.extend_from_slice(&record_crc(lsn, payload).to_le_bytes());
    out.extend_from_slice(payload);
}

/// Truncate `msg` to at most [`MAX_ERR_MSG`] bytes on a UTF-8 boundary.
fn bounded_msg(msg: &str) -> &str {
    if msg.len() <= MAX_ERR_MSG {
        return msg;
    }
    let mut end = MAX_ERR_MSG;
    while !msg.is_char_boundary(end) {
        end -= 1;
    }
    &msg[..end]
}

/// Append the encoding of `frame` to `out`. An `ERR` message longer than
/// [`MAX_ERR_MSG`] is truncated on a UTF-8 boundary (so it always decodes).
///
/// # Panics
/// If a `RECORD` payload is so large its length does not fit the `u32` `len`
/// field (> ~4 GiB) — a caller bug, since payloads are bounded by the WAL's
/// `u32` `max_record_size`.
pub fn encode_frame(frame: &Frame<'_>, out: &mut Vec<u8>) {
    match *frame {
        Frame::Hello {
            proto_ver,
            replica_id,
            durable_lsn,
        } => {
            put_header(out, T_HELLO, HELLO_BODY);
            out.extend_from_slice(&proto_ver.to_le_bytes());
            out.extend_from_slice(&replica_id.to_le_bytes());
            out.extend_from_slice(&durable_lsn.0.to_le_bytes());
        }
        Frame::Serving {
            from_lsn,
            primary_oldest,
        } => {
            put_header(out, T_SERVING, SERVING_BODY);
            out.extend_from_slice(&from_lsn.0.to_le_bytes());
            out.extend_from_slice(&primary_oldest.0.to_le_bytes());
        }
        Frame::ReseedRequired { primary_oldest } => {
            put_header(out, T_RESEED, LSN_BODY);
            out.extend_from_slice(&primary_oldest.0.to_le_bytes());
        }
        Frame::Record { lsn, payload } => encode_record(out, lsn, payload),
        Frame::Ack { durable_lsn } => {
            put_header(out, T_ACK, LSN_BODY);
            out.extend_from_slice(&durable_lsn.0.to_le_bytes());
        }
        Frame::Heartbeat { durable_lsn } => {
            put_header(out, T_HEARTBEAT, LSN_BODY);
            out.extend_from_slice(&durable_lsn.0.to_le_bytes());
        }
        Frame::Err { code, msg } => {
            let msg = bounded_msg(msg);
            put_header(out, T_ERR, 2 + msg.len());
            out.extend_from_slice(&(code as u16).to_le_bytes());
            out.extend_from_slice(msg.as_bytes());
        }
    }
}

/// Streaming frame reader over a byte source (a socket). Buffers at most one
/// maximal frame ([`Limits::max_len`]` + 4` bytes): an over-limit `len` is
/// rejected from its prefix before any body byte is read.
///
/// A read timeout or `WouldBlock` from the source surfaces as
/// [`ReplError::Io`] **without losing buffered bytes** — the next
/// [`read_frame`](FrameReader::read_frame) resumes the partial frame. This is
/// what lets the receiver use the socket read timeout as its group-commit
/// `batch_interval` clock (§7 step 5).
pub struct FrameReader<R> {
    inner: R,
    limits: Limits,
    buf: Vec<u8>,
    start: usize,
    end: usize,
}

/// Bytes requested from the source per `read` once the buffer is drained.
const READ_CHUNK: usize = 64 * 1024;

impl<R: Read> FrameReader<R> {
    /// Wrap `inner`, decoding frames bounded by `limits`.
    pub fn new(inner: R, limits: Limits) -> FrameReader<R> {
        FrameReader {
            inner,
            limits,
            buf: Vec::new(),
            start: 0,
            end: 0,
        }
    }

    /// The wrapped source.
    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    /// Read the next complete frame. Returns `Err(Io(UnexpectedEof))` on a
    /// clean or mid-frame EOF.
    pub fn read_frame(&mut self) -> Result<Frame<'_>> {
        loop {
            let avail = &self.buf[self.start..self.end];
            let need = match frame_len(avail, &self.limits)? {
                Some(total) if avail.len() >= total => {
                    let (s, e) = (self.start, self.start + total);
                    self.start = e;
                    return match decode_frame(&self.buf[s..e], &self.limits)? {
                        Decoded::Frame(f, _) => Ok(f),
                        Decoded::Incomplete { .. } => unreachable!("full frame present"),
                    };
                }
                Some(total) => total,
                None => LEN_PREFIX,
            };
            self.fill(need)?;
        }
    }

    /// Ensure room for a frame of `need` total bytes, then read once.
    fn fill(&mut self, need: usize) -> Result<()> {
        // Only reached with an incomplete frame buffered (`end - start < need`):
        // move it to the front so the buffer never exceeds one frame + a chunk.
        if self.start > 0 {
            self.buf.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }
        // `need` is bounded by `max_len + 4` (frame_len validated it) and
        // `end < need`, so the buffer never grows past one maximal frame plus a
        // read chunk — a hostile peer cannot inflate it.
        let want = need.max(self.end + READ_CHUNK);
        if self.buf.len() < want {
            self.buf.resize(want, 0);
        }
        let n = self.inner.read(&mut self.buf[self.end..])?;
        if n == 0 {
            return Err(ReplError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "peer closed the connection",
            )));
        }
        self.end += n;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const LIM: Limits = Limits::new(4096);

    fn enc(f: &Frame<'_>) -> Vec<u8> {
        let mut v = Vec::new();
        encode_frame(f, &mut v);
        v
    }

    fn all_frames(payload: &[u8]) -> Vec<Frame<'_>> {
        vec![
            Frame::Hello {
                proto_ver: PROTO_VERSION,
                replica_id: 0xDEAD_BEEF_0102_0304,
                durable_lsn: Lsn(42),
            },
            Frame::Serving {
                from_lsn: Lsn(43),
                primary_oldest: Lsn(7),
            },
            Frame::ReseedRequired {
                primary_oldest: Lsn(1000),
            },
            Frame::Record {
                lsn: Lsn(43),
                payload,
            },
            Frame::Record {
                lsn: Lsn(44),
                payload: &[],
            },
            Frame::Ack {
                durable_lsn: Lsn(u64::MAX),
            },
            Frame::Heartbeat {
                durable_lsn: Lsn(0),
            },
            Frame::Err {
                code: ErrCode::Contiguity,
                msg: "expected 5, got 9",
            },
            Frame::Err {
                code: ErrCode::Poisoned,
                msg: "",
            },
        ]
    }

    #[test]
    fn roundtrip_every_frame() {
        let payload: Vec<u8> = (0..=255u8).collect();
        for f in all_frames(&payload) {
            let bytes = enc(&f);
            assert_eq!(
                decode_frame(&bytes, &LIM).unwrap(),
                Decoded::Frame(f, bytes.len()),
                "{f:?}"
            );
        }
    }

    #[test]
    fn concatenated_frames_decode_in_sequence() {
        let payload = b"hello replica".to_vec();
        let frames = all_frames(&payload);
        let mut stream = Vec::new();
        for f in &frames {
            encode_frame(f, &mut stream);
        }
        let mut at = 0;
        for f in &frames {
            match decode_frame(&stream[at..], &LIM).unwrap() {
                Decoded::Frame(got, n) => {
                    assert_eq!(&got, f);
                    at += n;
                }
                Decoded::Incomplete { .. } => panic!("incomplete"),
            }
        }
        assert_eq!(at, stream.len());
    }

    #[test]
    fn record_crc_covers_lsn_and_payload() {
        let c = record_crc(Lsn(1), b"abc");
        assert_ne!(c, record_crc(Lsn(2), b"abc"));
        assert_ne!(c, record_crc(Lsn(1), b"abd"));
        // Spec: crc32c(lsn_le ‖ payload) — check against a one-shot computation.
        let mut cat = 1u64.to_le_bytes().to_vec();
        cat.extend_from_slice(b"abc");
        assert_eq!(c, crc32c::crc32c(&cat));
    }

    #[test]
    fn flipping_any_record_body_byte_is_wire_crc() {
        let bytes = enc(&Frame::Record {
            lsn: Lsn(77),
            payload: b"payload bytes",
        });
        // Body = everything after len(4) + type(1): lsn, crc, payload.
        for i in (LEN_PREFIX + 1)..bytes.len() {
            for bit in 0..8 {
                let mut b = bytes.clone();
                b[i] ^= 1 << bit;
                assert!(
                    matches!(decode_frame(&b, &LIM), Err(ReplError::WireCrc)),
                    "byte {i} bit {bit}"
                );
            }
        }
    }

    #[test]
    fn flipping_header_bytes_never_yields_a_different_record() {
        let payload = b"0123456789abcdef";
        let orig = Frame::Record {
            lsn: Lsn(5),
            payload,
        };
        let bytes = enc(&orig);
        for i in 0..=LEN_PREFIX {
            for bit in 0..8 {
                let mut b = bytes.clone();
                b[i] ^= 1 << bit;
                if let Ok(Decoded::Frame(Frame::Record { lsn, payload: p }, _)) =
                    decode_frame(&b, &LIM)
                {
                    panic!("corrupt header decoded as RECORD {lsn:?} {p:?} (byte {i} bit {bit})");
                }
            }
        }
    }

    #[test]
    fn short_frames_are_incomplete_never_errors_or_panics() {
        let bytes = enc(&Frame::Record {
            lsn: Lsn(9),
            payload: &[7u8; 100],
        });
        for cut in 0..bytes.len() {
            match decode_frame(&bytes[..cut], &LIM).unwrap() {
                Decoded::Incomplete { total } => {
                    assert!(total == LEN_PREFIX || total == bytes.len(), "cut {cut}");
                }
                Decoded::Frame(..) => panic!("frame from a {cut}-byte prefix"),
            }
        }
    }

    #[test]
    fn oversize_len_is_rejected_from_the_prefix_alone() {
        // Only the 4-byte prefix is present: the decoder must reject the claimed
        // length without waiting for (or buffering) a 4 GiB body.
        let max = LIM.max_len();
        let over = u32::try_from(max + 1).unwrap();
        assert!(matches!(
            decode_frame(&over.to_le_bytes(), &LIM),
            Err(ReplError::Protocol(_))
        ));
        assert!(matches!(
            decode_frame(&u32::MAX.to_le_bytes(), &LIM),
            Err(ReplError::Protocol(_))
        ));
        assert!(matches!(
            decode_frame(&0u32.to_le_bytes(), &LIM),
            Err(ReplError::Protocol(_))
        ));
        // Exactly at the limit is accepted as a (still incomplete) frame.
        let at = u32::try_from(max).unwrap();
        assert_eq!(
            decode_frame(&at.to_le_bytes(), &LIM).unwrap(),
            Decoded::Incomplete {
                total: max as usize + LEN_PREFIX
            }
        );
    }

    #[test]
    fn record_payload_over_max_is_protocol_error() {
        let bytes = enc(&Frame::Record {
            lsn: Lsn(1),
            payload: &[0u8; 65],
        });
        assert!(matches!(
            decode_frame(&bytes, &Limits::new(64)),
            Err(ReplError::Protocol(_))
        ));
        assert!(decode_frame(&bytes, &Limits::new(65)).is_ok());
    }

    #[test]
    fn wrong_fixed_sizes_and_unknown_types_are_protocol_errors() {
        // ACK with a 9-byte body.
        let mut b = vec![10, 0, 0, 0, T_ACK];
        b.extend_from_slice(&[0; 9]);
        assert!(matches!(
            decode_frame(&b, &LIM),
            Err(ReplError::Protocol(_))
        ));
        // HELLO with a 17-byte body.
        let mut b = vec![18, 0, 0, 0, T_HELLO];
        b.extend_from_slice(&[0; 17]);
        assert!(matches!(
            decode_frame(&b, &LIM),
            Err(ReplError::Protocol(_))
        ));
        // RECORD shorter than lsn+crc.
        let mut b = vec![12, 0, 0, 0, T_RECORD];
        b.extend_from_slice(&[0; 11]);
        assert!(matches!(
            decode_frame(&b, &LIM),
            Err(ReplError::Protocol(_))
        ));
        // Unknown types 0 and 8..=255.
        for ty in std::iter::once(0u8).chain(8..=255) {
            let b = [9, 0, 0, 0, ty, 0, 0, 0, 0, 0, 0, 0, 0];
            assert!(
                matches!(decode_frame(&b, &LIM), Err(ReplError::Protocol(_))),
                "type {ty}"
            );
        }
    }

    #[test]
    fn err_frame_bounds() {
        // Unknown code.
        let b = [3, 0, 0, 0, T_ERR, 99, 0];
        assert!(matches!(
            decode_frame(&b, &LIM),
            Err(ReplError::Protocol(_))
        ));
        // Non-UTF-8 message.
        let b = [5, 0, 0, 0, T_ERR, 1, 0, 0xFF, 0xFE];
        assert!(matches!(
            decode_frame(&b, &LIM),
            Err(ReplError::Protocol(_))
        ));
        // Over-long message on the wire.
        let mut b = Vec::new();
        put_header(&mut b, T_ERR, 2 + MAX_ERR_MSG + 1);
        b.extend_from_slice(&1u16.to_le_bytes());
        b.extend(std::iter::repeat_n(b'x', MAX_ERR_MSG + 1));
        assert!(matches!(
            decode_frame(&b, &LIM),
            Err(ReplError::Protocol(_))
        ));
        // Encoding truncates on a char boundary, so it always decodes.
        let long = "é".repeat(MAX_ERR_MSG); // 2 bytes per char
        let bytes = enc(&Frame::Err {
            code: ErrCode::Protocol,
            msg: &long,
        });
        match decode_frame(&bytes, &LIM).unwrap() {
            Decoded::Frame(Frame::Err { msg, .. }, _) => {
                assert!(msg.len() <= MAX_ERR_MSG && long.starts_with(msg));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn frame_reader_reassembles_byte_at_a_time_and_reports_eof() {
        struct Trickle(Vec<u8>, usize);
        impl Read for Trickle {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                if self.1 >= self.0.len() || out.is_empty() {
                    return Ok(0);
                }
                out[0] = self.0[self.1];
                self.1 += 1;
                Ok(1)
            }
        }
        let payload = vec![3u8; 300];
        let frames = all_frames(&payload);
        let mut stream = Vec::new();
        for f in &frames {
            encode_frame(f, &mut stream);
        }
        let mut r = FrameReader::new(Trickle(stream, 0), LIM);
        for f in &frames {
            assert_eq!(&r.read_frame().unwrap(), f);
        }
        match r.read_frame() {
            Err(ReplError::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn frame_reader_keeps_partial_bytes_across_a_timeout() {
        // A source that returns WouldBlock mid-frame must not lose bytes.
        struct Stutter {
            chunks: Vec<Option<Vec<u8>>>,
        }
        impl Read for Stutter {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                match self.chunks.first().cloned() {
                    None => Ok(0),
                    Some(None) => {
                        self.chunks.remove(0);
                        Err(io::ErrorKind::WouldBlock.into())
                    }
                    Some(Some(c)) => {
                        let n = c.len().min(out.len());
                        out[..n].copy_from_slice(&c[..n]);
                        if n == c.len() {
                            self.chunks.remove(0);
                        } else {
                            self.chunks[0] = Some(c[n..].to_vec());
                        }
                        Ok(n)
                    }
                }
            }
        }
        let bytes = enc(&Frame::Record {
            lsn: Lsn(3),
            payload: b"split across a timeout",
        });
        let (a, b) = bytes.split_at(9);
        let mut r = FrameReader::new(
            Stutter {
                chunks: vec![Some(a.to_vec()), None, Some(b.to_vec())],
            },
            LIM,
        );
        match r.read_frame() {
            Err(ReplError::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::WouldBlock),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            r.read_frame().unwrap(),
            Frame::Record {
                lsn: Lsn(3),
                payload: b"split across a timeout"
            }
        );
    }

    #[test]
    fn frame_reader_rejects_oversize_without_buffering_it() {
        let mut stream = u32::MAX.to_le_bytes().to_vec();
        stream.extend_from_slice(&[0u8; 16]);
        let mut r = FrameReader::new(&stream[..], LIM);
        assert!(matches!(r.read_frame(), Err(ReplError::Protocol(_))));
        assert!(r.buf.len() <= READ_CHUNK);
    }

    proptest! {
        #[test]
        fn arbitrary_bytes_never_panic_and_stay_in_bounds(
            bytes in proptest::collection::vec(any::<u8>(), 0..300),
            max_payload in prop_oneof![Just(0u32), Just(1), Just(64), Just(4096), Just(u32::MAX)],
        ) {
            let lim = Limits::new(max_payload);
            match decode_frame(&bytes, &lim) {
                Ok(Decoded::Frame(f, n)) => {
                    prop_assert!(n <= bytes.len());
                    prop_assert!(n > LEN_PREFIX);
                    if let Frame::Record { payload, .. } = f {
                        prop_assert!(payload.len() as u64 <= u64::from(max_payload));
                    }
                    // A decoded frame re-encodes to exactly the bytes consumed.
                    prop_assert_eq!(enc(&f), bytes[..n].to_vec());
                }
                Ok(Decoded::Incomplete { total }) => {
                    prop_assert!(total > bytes.len());
                    prop_assert!(total as u64 <= lim.max_len() + LEN_PREFIX as u64);
                }
                Err(_) => {}
            }
        }

        #[test]
        fn record_roundtrip(lsn in any::<u64>(), payload in proptest::collection::vec(any::<u8>(), 0..512)) {
            let mut v = Vec::new();
            encode_record(&mut v, Lsn(lsn), &payload);
            prop_assert_eq!(
                decode_frame(&v, &LIM).unwrap(),
                Decoded::Frame(Frame::Record { lsn: Lsn(lsn), payload: &payload }, v.len())
            );
        }
    }
}
