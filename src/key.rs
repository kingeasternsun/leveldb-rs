#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]

use crate::GenericResult;
use bytes::{Buf, BufMut};
use std::fmt;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KeyError {
    #[error("leveldb: internal key {ikey:?} corrupted: {reason:?}")]
    InternalKeyCorrupted { ikey: Vec<u8>, reason: String },
    #[error("invalid keytype :`{0}`")]
    InvalidKeyType(u8),
    #[error("unknown data store error")]
    Unknown,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum keyType {
    Del = 0,
    Val = 1,
    Seek = 2,
}

impl fmt::Display for keyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            keyType::Del => write!(f, "d"),
            keyType::Val => write!(f, "v"),
            keyType::Seek => write!(f, "s"),
        }
    }
}
pub const KEY_MAX_SEQ: u64 = (1 << 56) - 1;
// const (
// 	// Maximum value possible for sequence number; the 8-bits are
// 	// used by value type, so its can packed together in single
// 	// 64-bit integer.
// 	keyMaxSeq = (uint64(1) << 56) - 1
// 	// Maximum value possible for packed sequence number and type.
// 	keyMaxNum = (keyMaxSeq << 8) | uint64(keyTypeSeek)
// )
pub fn keyTypeFromU8(n: u8) -> GenericResult<keyType> {
    match n {
        0 => Ok(keyType::Del),
        1 => Ok(keyType::Val),
        _ => Err(Box::new(KeyError::InvalidKeyType(n))),
    }
}

// type internalKey []byte

// func makeInternalKey(dst, ukey []byte, seq uint64, kt keyType) internalKey {
// 	if seq > keyMaxSeq {
// 		panic("leveldb: invalid sequence number")
// 	} else if kt > keyTypeVal {
// 		panic("leveldb: invalid type")
// 	}

// 	dst = ensureBuffer(dst, len(ukey)+8)
// 	copy(dst, ukey)
// 	binary.LittleEndian.PutUint64(dst[len(ukey):], (seq<<8)|uint64(kt))
// 	return internalKey(dst)
// }
#[derive(Debug, Clone)]
pub struct internalKey(Vec<u8>);
// 将用户的key 添加 sequence number 和 keyType 变为 internalKey
pub fn makeInternalKey(ukey: &[u8], seq: u64, kt: keyType) -> internalKey {
    let mut v = ukey.to_vec();
    let mut seq = seq << 8;
    if kt == keyType::Val || kt == keyType::Seek {
        seq = seq | 1
    }
    v.put_u64_le(seq);
    internalKey(v)
}

// func parseInternalKey(ik []byte) (ukey []byte, seq uint64, kt keyType, err error) {
// 	if len(ik) < 8 {
// 		return nil, 0, 0, newErrInternalKeyCorrupted(ik, "invalid length")
// 	}
// 	num := binary.LittleEndian.Uint64(ik[len(ik)-8:])
// 	seq, kt = uint64(num>>8), keyType(num&0xff)
// 	if kt > keyTypeVal {
// 		return nil, 0, 0, newErrInternalKeyCorrupted(ik, "invalid type")
// 	}
// 	ukey = ik[:len(ik)-8]
// 	return
// }
// 从 internalKey 中解析后用户的key sequence number 和 keyType
pub fn parseInternalKey(ik: &[u8]) -> GenericResult<(Vec<u8>, u64, keyType)> {
    let mut res = (Vec::new(), 0, keyType::Del);
    if ik.len() < 8 {
        return Err(Box::new(KeyError::InternalKeyCorrupted {
            ikey: ik.to_vec(),
            reason: String::from("invalid length"),
        }));
    }

    let mut b = &ik[ik.len() - 8..ik.len()];
    let num = b.get_u64_le();
    res.1 = num >> 8;
    res.2 = keyTypeFromU8((num & 0xff) as u8)?;
    res.0 = ik[..ik.len() - 8].to_vec();

    Ok(res)
}

impl internalKey {
    pub fn new(data: &[u8]) -> Self {
        internalKey(data.to_vec())
    }

    pub fn ukey(&self) -> &[u8] {
        assert!(self.0.len() > 8);
        return &self.0[..self.0.len() - 8];
    }
    pub fn data(&self) -> &[u8] {
        return &self.0[..];
    }

    pub fn num(&self) -> u64 {
        assert!(self.0.len() > 8);
        let mut buf = &self.0[self.0.len() - 8..];
        buf.get_u64_le()
    }

    fn parseNum(&self) -> GenericResult<(u64, keyType)> {
        let num = self.num();
        let mut res = (0, keyType::Del);
        res.0 = num >> 8;
        res.1 = keyTypeFromU8((num & 0xff) as u8)?;
        Ok(res)
    }
}
