//! On-disk record framing.
//!
//! Each record is a self-describing frame so a log can be replayed after a
//! crash, including a crash that leaves a partial frame at the tail:
//!
//! ```text
//! +-------------+-------------+-------------+-------------------+
//! | magic (u32) | len   (u32) | crc   (u32) | payload (len B)   |
//! +-------------+-------------+-------------+-------------------+
//! ```
//!
//! All integers are little endian. `crc` is the IEEE CRC-32 of the payload,
//! which catches both torn writes and bit rot. A short header or a payload
//! shorter than `len` means the frame never made it through a flush and must
//! be ignored on recovery.

use std::fmt;

/// Magic prefix for every frame, ASCII `GCMT`.
pub const MAGIC: u32 = 0x4743_4D54;

/// Fixed header size in bytes: magic + length + crc.
pub const HEADER_LEN: usize = 12;

/// Maximum payload length representable by the `u32` length field.
pub const MAX_PAYLOAD_LEN: usize = u32::MAX as usize;

/// A decoded WAL record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
  payload: Vec<u8>,
}

impl Record {
  /// Wraps an already decoded payload.
  #[inline]
  pub fn new(payload: Vec<u8>) -> Self {
    Self { payload }
  }

  /// The record's payload bytes.
  #[inline]
  pub fn payload(&self) -> &[u8] {
    &self.payload
  }

  /// Consumes the record and returns the payload bytes.
  #[inline]
  pub fn into_payload(self) -> Vec<u8> {
    self.payload
  }
}

/// CRC values did not match, so the frame is corrupt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrcMismatch {
  /// CRC stored in the frame header.
  pub expected: u32,
  /// CRC computed from the payload.
  pub found: u32,
}

impl fmt::Display for CrcMismatch {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "crc mismatch: header has {:#x}, payload gives {:#x}",
      self.expected, self.found
    )
  }
}

impl std::error::Error for CrcMismatch {}

/// Failure while decoding frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
  /// The frame did not start with [`MAGIC`]; the offset points at the bad
  /// byte so callers can decide whether to resynchronize.
  BadMagic {
    /// Byte offset of the invalid magic.
    offset: usize,
  },
  /// The payload length exceeds [`MAX_PAYLOAD_LEN`].
  PayloadTooLong {
    /// Declared, impossible length.
    len: usize,
  },
  /// The checksum did not match.
  Crc(CrcMismatch),
  /// The buffer ends in the middle of a frame, i.e. a torn write.
  TornTail {
    /// Number of intact bytes preceding the torn frame.
    valid_bytes: usize,
  },
}

impl fmt::Display for FrameError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::BadMagic { offset } => write!(f, "bad frame magic at offset {offset}"),
      Self::PayloadTooLong { len } => write!(f, "payload length {len} is too large"),
      Self::Crc(err) => write!(f, "{err}"),
      Self::TornTail { valid_bytes } => {
        write!(f, "torn frame after {valid_bytes} valid bytes")
      }
    }
  }
}

impl std::error::Error for FrameError {}

/// Encodes `payload` into one framed buffer ready to be appended to the log.
pub fn encode(payload: &[u8]) -> Vec<u8> {
  let len = payload.len() as u32;
  let crc = crc32(payload);
  let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
  out.extend_from_slice(&MAGIC.to_le_bytes());
  out.extend_from_slice(&len.to_le_bytes());
  out.extend_from_slice(&crc.to_le_bytes());
  out.extend_from_slice(payload);
  out
}

/// Decodes a single frame at the start of `buf`, returning the record and the
/// total number of bytes the frame occupies.
pub fn decode_one(buf: &[u8]) -> Result<(Record, usize), FrameError> {
  if buf.len() < HEADER_LEN {
    return Err(FrameError::TornTail { valid_bytes: 0 });
  }

  let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
  if magic != MAGIC {
    return Err(FrameError::BadMagic { offset: 0 });
  }

  let len = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
  if len > MAX_PAYLOAD_LEN {
    return Err(FrameError::PayloadTooLong { len });
  }

  let frame_len = HEADER_LEN + len;
  if buf.len() < frame_len {
    return Err(FrameError::TornTail { valid_bytes: 0 });
  }

  let expected = u32::from_le_bytes(buf[8..12].try_into().unwrap());
  let payload = &buf[HEADER_LEN..frame_len];
  let found = crc32(payload);
  if found != expected {
    return Err(FrameError::Crc(CrcMismatch { expected, found }));
  }

  Ok((Record::new(payload.to_vec()), frame_len))
}

/// Strictly decodes every frame in `buf`; fails on the first torn or corrupt
/// frame. Use this when the log is expected to be intact.
pub fn decode_all(buf: &[u8]) -> Result<Vec<Record>, FrameError> {
  let mut records = Vec::new();
  let mut offset = 0;
  while offset < buf.len() {
    match decode_one(&buf[offset..]) {
      Ok((record, frame_len)) => {
        records.push(record);
        offset += frame_len;
      }
      Err(FrameError::TornTail { .. }) | Err(FrameError::BadMagic { .. }) => {
        return Err(FrameError::TornTail {
          valid_bytes: offset,
        });
      }
      Err(err) => return Err(err),
    }
  }
  Ok(records)
}

/// Recovery-oriented decode: walks intact frames and stops at the first torn
/// tail or checksum failure. Returns the decoded records together with the
/// number of bytes that are known good, which is exactly how long the log
/// should be truncated before appending new frames.
pub fn decode_recoverable(buf: &[u8]) -> (Vec<Record>, usize) {
  let mut records = Vec::new();
  let mut offset = 0;
  while offset < buf.len() {
    match decode_one(&buf[offset..]) {
      Ok((record, frame_len)) => {
        records.push(record);
        offset += frame_len;
      }
      Err(_) => break,
    }
  }
  (records, offset)
}

const CRC_TABLE: [u32; 256] = {
  let mut table = [0u32; 256];
  let mut i = 0;
  while i < 256 {
    let mut crc = i as u32;
    let mut j = 0;
    while j < 8 {
      crc = if crc & 1 != 0 {
        0xEDB8_8320 ^ (crc >> 1)
      } else {
        crc >> 1
      };
      j += 1;
    }
    table[i] = crc;
    i += 1;
  }
  table
};

fn crc32(data: &[u8]) -> u32 {
  let mut crc = 0xFFFF_FFFFu32;
  for &byte in data {
    let index = ((crc ^ byte as u32) & 0xFF) as usize;
    crc = CRC_TABLE[index] ^ (crc >> 8);
  }
  crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn roundtrip_single_and_many() {
    let frames = [
      encode(b""),
      encode(b"order 1"),
      encode(vec![0xAB; 300].as_slice()),
    ];
    let mut buf = Vec::new();
    for frame in &frames {
      buf.extend_from_slice(frame);
    }

    let records = decode_all(&buf).unwrap();
    assert_eq!(records.len(), 3);
    assert!(records[0].payload().is_empty());
    assert_eq!(records[1].payload(), b"order 1");
    assert_eq!(records[2].payload(), vec![0xAB; 300]);

    let (again, valid) = decode_recoverable(&buf);
    assert_eq!(again.len(), 3);
    assert_eq!(valid, buf.len());
  }

  #[test]
  fn torn_tail_is_recoverable() {
    let mut buf = encode(b"committed-1");
    let good_len = buf.len();
    buf.extend_from_slice(&encode(b"committed-2"));
    buf.extend_from_slice(&encode(b"torn"));
    buf.truncate(buf.len() - 3);

    let err = decode_all(&buf).unwrap_err();
    assert!(matches!(err, FrameError::TornTail { valid_bytes } if valid_bytes == good_len * 2));

    let (records, valid) = decode_recoverable(&buf);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload(), b"committed-1");
    assert_eq!(records[1].payload(), b"committed-2");
    assert_eq!(valid, good_len * 2);
  }

  #[test]
  fn corrupt_payload_fails_crc() {
    let mut buf = encode(b"order 42");
    let last = buf.len() - 1;
    buf[last] ^= 0xFF;
    assert!(matches!(decode_one(&buf), Err(FrameError::Crc(_))));
    let (records, valid) = decode_recoverable(&buf);
    assert!(records.is_empty());
    assert_eq!(valid, 0);
  }

  #[test]
  fn empty_buffer_decodes_to_nothing() {
    assert!(decode_all(&[]).unwrap().is_empty());
    let (records, valid) = decode_recoverable(&[]);
    assert!(records.is_empty());
    assert_eq!(valid, 0);
  }
}
