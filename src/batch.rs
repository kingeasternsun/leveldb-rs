#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
use crate::coding::GetVarint64Ptr;
use crate::key::internalKey;
use crate::key::keyType;
use crate::key::makeInternalKey;
use crate::memdb;
use crate::BytesComparer;
use crate::GenericResult;
use crate::{coding::MAX_VARINT_LEN64, key::keyTypeFromU8};
use bytes::{Buf, BufMut, BytesMut};
use std::io::Write;
use std::{io::Cursor, option::Option};

// ErrBatchCorrupted records reason of batch corruption. This error will be
// wrapped with errors.ErrCorrupted.
// type ErrBatchCorrupted struct {
// 	Reason string
// }

// func (e *ErrBatchCorrupted) Error() string {
// 	return fmt.Sprintf("leveldb: batch corrupted: %s", e.Reason)
// }

// func newErrBatchCorrupted(reason string) error {
// 	return errors.NewErrCorrupted(storage.FileDesc{}, &ErrBatchCorrupted{reason})
// }
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BatchError {
    #[error("decode varint error")]
    InvalidVarint,
    #[error("leveldb: batch corrupted:`{0}`")]
    BatchCorrupted(String),
    #[error("invalid key len (expected {expected:?}, found {found:?})")]
    InvalidKeyLen { expected: usize, found: usize },
    #[error("invalid value len (expected {expected:?}, found {found:?})")]
    InvalidValueLen { expected: usize, found: usize },
    #[error("invalid header (expected {expected:?}, found {found:?})")]
    InvalidHeader { expected: String, found: String },
    #[error("invalid indexlen (expected {expected:?}, found {found:?})")]
    InvalidIndexLen { expected: usize, found: usize },
    #[error("invalid seq number (expected {expected:?}, found {found:?})")]
    InvalidSeqNum { expected: u64, found: u64 },
    #[error("invalid batch number (expected {expected:?}, found {found:?})")]
    InvalidBatchNum { expected: u32, found: u32 },
    #[error("unknown data store error")]
    Unknown,
}

const BATCH_HEADER_LEN: u32 = 8 + 4;
const BATCH_GROW_REC: u32 = 3000;
const BATCH_BUFIO_SIZE: u32 = 16;

pub trait BatchReplay {
    fn Put(&mut self, key: &[u8], value: Option<&[u8]>);
    fn Delete(&mut self, key: &[u8]);
}
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct batchIndex {
    key_type: keyType,
    Key: (usize, usize),   // 指向key所在的起始位置和长度
    Value: (usize, usize), // 指向Value所在的起始位置和长度
}

impl batchIndex {
    fn new(kt: keyType) -> Self {
        Self {
            key_type: kt,
            Key: (0, 0),
            Value: (0, 0),
        }
    }
    // 返回data中的key, 注意要使用lifetime
    fn k<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        return &data[self.Key.0..self.Key.1];
    }

    // 返回data中的 value
    fn v<'a>(&self, data: &'a [u8]) -> Option<&'a [u8]> {
        if self.Value.1 != 0 {
            Some(&data[self.Value.0..self.Value.1])
        } else {
            None
        }
    }

    // 返回 data中的key 和 value
    fn kv<'a>(&self, data: &'a [u8]) -> (&'a [u8], Option<&'a [u8]>) {
        return (self.k(data), self.v(data));
    }
}

pub struct Batch {
    data: BytesMut,
    index: Vec<batchIndex>,
    internalLen: usize,
}

impl Batch {
    fn new() -> Self {
        Batch {
            data: BytesMut::new(),
            index: Vec::new(),
            internalLen: 0,
        }
    }
    fn encode_varint64(&mut self, v: u64) {
        let B: u8 = 128;
        let mut v = v;

        while v > B as u64 {
            self.data.put_u8(v as u8 | B);
            v = v >> 7;
        }

        self.data.put_u8(v as u8);
    }

    //func (b *Batch) appendRec(kt keyType, key, value []byte) {
    // 	n := 1 + binary.MaxVarintLen32 + len(key)
    // 	if kt == keyTypeVal {
    // 		n += binary.MaxVarintLen32 + len(value)
    // 	}
    // 	b.grow(n)
    // 	index := batchIndex{keyType: kt}
    // 	o := len(b.data)
    // 	data := b.data[:o+n]
    // 	data[o] = byte(kt)
    // 	o++
    // 	o += binary.PutUvarint(data[o:], uint64(len(key)))
    // 	index.keyPos = o
    // 	index.keyLen = len(key)
    // 	o += copy(data[o:], key)
    // 	if kt == keyTypeVal {
    // 		o += binary.PutUvarint(data[o:], uint64(len(value)))
    // 		index.valuePos = o
    // 		index.valueLen = len(value)
    // 		o += copy(data[o:], value)
    // 	}
    // 	b.data = data[:o]
    // 	b.index = append(b.index, index)
    // 	b.internalLen += index.keyLen + index.valueLen + 8
    // }
    pub fn append_rec(&mut self, kt: keyType, key: &[u8], value: &[u8]) {
        self.data.reserve(1 + MAX_VARINT_LEN64 + key.len());
        if kt == keyType::Val {
            self.data.reserve(MAX_VARINT_LEN64 + value.len());
        }

        let mut index = batchIndex::new(kt);
        // 写入 keyType
        self.data.put_u8(kt as u8);
        // 写入 key的长度

        // 如果使用下面这种，数据的写入没有办法反馈到外层self.data 中
        // let v_len = EncodeVarint64(&mut self.data[o + 1..], key.len() as u64);
        // 或者只有定义一个额外的Vec，然后传入到 EncodeVarint64，再使用 put_slice将数据写入self.data 多了一层数据拷贝
        // 不如直接对self.data 进行写入
        self.encode_varint64(key.len() as u64);

        // 这时候，内部指针所指向的位置 ，就是key要写入的起始位置，更新 index.Key
        index.Key = (self.data.len(), key.len());
        // 写入 key
        self.data.put_slice(key);

        if kt == keyType::Val {
            // 写入 value的长度
            self.encode_varint64(value.len() as u64);
            // 这时候，内部指针指向的位置，就是value要写入的起始位置，更新index.Value
            index.Value = (self.data.len(), value.len());
            // 写入value
            self.data.put_slice(value);
        }
        self.internalLen += index.Key.1 + index.Value.1 + 8;
        self.index.push(index);
    }

    // Put appends 'put operation' of the given key/value pair to the batch.
    // It is safe to modify the contents of the argument after Put returns but not
    // before.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.append_rec(keyType::Val, key, value)
    }

    // Delete appends 'delete operation' of the given key to the batch.
    // It is safe to modify the contents of the argument after Delete returns but
    // not before.
    pub fn delete(&mut self, key: &[u8]) {
        self.append_rec(keyType::Del, key, &vec![])
    }

    // Dump dumps batch contents. The returned slice can be loaded into the
    // batch using Load method.
    // The returned slice is not its own copy, so the contents should not be
    // modified.
    pub fn dump(&self) -> &[u8] {
        &self.data[..]
    }
    // Load loads given slice into the batch. Previous contents of the batch
    // will be discarded.
    // The given slice will not be copied and will be used as batch buffer, so
    // it is not safe to modify the contents of the slice.
    // pub fn load(&mut self, buf: &[u8]) -> Option<()> {
    //     self.decode(buf, 0)
    // }

    // Replay replays batch contents.
    pub fn replay<T>(&self, r: &mut T)
    where
        T: BatchReplay,
    {
        for index in &self.index {
            match index.key_type {
                keyType::Val => r.Put(index.k(&self.data[..]), index.v(&self.data[..])),
                keyType::Del => r.Delete(index.k(&self.data[..])),
                keyType::Seek => {}
            }
        }
    }

    // Len returns number of records in the batch.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    // Reset resets the batch.
    pub fn reset(&mut self) {
        self.data.clear();
        self.index.clear();
        self.internalLen = 0;
    }

    // func (b *Batch) replayInternal(fn func(i int, kt keyType, k, v []byte) error) error {
    // 	for i, index := range b.index {
    // 		if err := fn(i, index.keyType, index.k(b.data), index.v(b.data)); err != nil {
    // 			return err
    // 		}
    // 	}
    // 	return nil
    // }

    pub fn replay_internal<T, F>(&self, f: F) -> GenericResult<()>
    where
        F: Fn(usize, keyType, &[u8], Option<&[u8]>) -> GenericResult<T>,
    {
        for (i, index) in self.index.iter().enumerate() {
            f(
                i,
                index.key_type,
                index.k(&self.data[..]),
                index.v(&self.data[..]),
            )?;
        }
        Ok(())
    }

    // func (b *Batch) append(p *Batch) {
    // 	ob := len(b.data)
    // 	oi := len(b.index)
    // 	b.data = append(b.data, p.data...)
    // 	b.index = append(b.index, p.index...)
    // 	b.internalLen += p.internalLen

    // 	// Updating index offset.
    // 	if ob != 0 {
    // 		for ; oi < len(b.index); oi++ {
    // 			index := &b.index[oi]
    // 			index.keyPos += ob
    // 			if index.valueLen != 0 {
    // 				index.valuePos += ob
    // 			}
    // 		}
    // 	}
    // }

    pub fn append(&mut self, p: &Self) {
        let ob = self.data.len();
        let oi = self.index.len();
        self.data.extend_from_slice(&p.data[..]);
        self.index.append(&mut p.index.clone());
        self.internalLen += p.internalLen;
        if ob != 0 {
            for index in self.index.iter_mut().skip(ob) {
                index.Key.0 += ob;
                if index.Value.1 != 0 {
                    index.Value.0 += ob;
                }
            }
        }
    }

    // func (b *Batch) decode(data []byte, expectedLen int) error {
    // 	b.data = data
    // 	b.index = b.index[:0]
    // 	b.internalLen = 0
    // 	err := decodeBatch(data, func(i int, index batchIndex) error {
    // 		b.index = append(b.index, index)
    // 		b.internalLen += index.keyLen + index.valueLen + 8
    // 		return nil
    // 	})
    // 	if err != nil {
    // 		return err
    // 	}
    // 	if expectedLen >= 0 && len(b.index) != expectedLen {
    // 		return newErrBatchCorrupted(fmt.Sprintf("invalid records length: %d vs %d", expectedLen, len(b.index)))
    // 	}
    // 	return nil
    // }
    pub fn decode(&mut self, buf: &[u8], expectedLen: usize) -> GenericResult<()> {
        self.index.clear();
        self.internalLen = 0;

        decode_batch(buf, |_i, index| {
            self.internalLen += index.Key.1 + index.Value.1 + 8;
            self.index.push(index);
            Ok(())
        })?;

        if expectedLen > 0 && self.index.len() != expectedLen {
            return Err(Box::new(BatchError::InvalidIndexLen {
                expected: expectedLen,
                found: self.index.len(),
            }));
        }

        Ok(())
    }

    // func (b *Batch) putMem(seq uint64, mdb *memdb.DB) error {
    // 	var ik []byte
    // 	for i, index := range b.index {
    // 		ik = makeInternalKey(ik, index.k(b.data), seq+uint64(i), index.keyType)
    // 		if err := mdb.Put(ik, index.v(b.data)); err != nil {
    // 			return err
    // 		}
    // 	}
    // 	return nil
    // }

    // 将数据放入到memdb，内存数据库中
    fn put_mem(&mut self, seq: u64, mdb: &mut memdb::Db<BytesComparer>) -> GenericResult<()> {
        let mut ik;
        for (i, index) in self.index.iter().enumerate() {
            ik = makeInternalKey(index.k(&self.data[..]), seq + i as u64, index.key_type);
            mdb.put(&ik, index.v(&self.data[..]))?;
        }
        Ok(())
    }
    // func (b *Batch) revertMem(seq uint64, mdb *memdb.DB) error {
    // 	var ik []byte
    // 	for i, index := range b.index {
    // 		ik = makeInternalKey(ik, index.k(b.data), seq+uint64(i), index.keyType)
    // 		if err := mdb.Delete(ik); err != nil {
    // 			return err
    // 		}
    // 	}
    // 	return nil
    // }
    fn revert_mem(&mut self, seq: u64, mdb: &mut memdb::Db<BytesComparer>) -> GenericResult<()> {
        let mut ik;
        for (i, index) in self.index.iter().enumerate() {
            ik = makeInternalKey(index.k(&self.data[..]), seq + i as u64, index.key_type);
            mdb.delete(&ik);
        }
        Ok(())
    }
}
// func decodeBatch(data []byte, fn func(i int, index batchIndex) error) error {
// 	var index batchIndex
// 	for i, o := 0, 0; o < len(data); i++ {
// 		// Key type.
// 		index.keyType = keyType(data[o])
// 		if index.keyType > keyTypeVal {
// 			return newErrBatchCorrupted(fmt.Sprintf("bad record: invalid type %#x", uint(index.keyType)))
// 		}
// 		o++

// 		// Key.
// 		x, n := binary.Uvarint(data[o:])
// 		o += n
// 		if n <= 0 || o+int(x) > len(data) {
// 			return newErrBatchCorrupted("bad record: invalid key length")
// 		}
// 		index.keyPos = o
// 		index.keyLen = int(x)
// 		o += index.keyLen

// 		// Value.
// 		if index.keyType == keyTypeVal {
// 			x, n = binary.Uvarint(data[o:])
// 			o += n
// 			if n <= 0 || o+int(x) > len(data) {
// 				return newErrBatchCorrupted("bad record: invalid value length")
// 			}
// 			index.valuePos = o
// 			index.valueLen = int(x)
// 			o += index.valueLen
// 		} else {
// 			index.valuePos = 0
// 			index.valueLen = 0
// 		}

// 		if err := fn(i, index); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }
/// 注意这里的buf的参数定义
pub fn decode_batch<F>(buf: &[u8], mut f: F) -> GenericResult<()>
where
    F: FnMut(usize, batchIndex) -> GenericResult<()>,
{
    let mut buf = Cursor::new(buf);
    let mut index = batchIndex::new(keyType::Del);
    let mut cnt = 0;
    loop {
        cnt += 1;
        if !buf.has_remaining() {
            return Ok(());
        }

        if buf.remaining() < 2 {
            return Err(Box::new(BatchError::BatchCorrupted(String::from(
                "bad record: invalid key length",
            ))));
        }

        // 读取key类型
        index.key_type = keyTypeFromU8(buf.get_u8())?;
        // 读取key的长度
        index.Key.1 = GetVarint64Ptr(&mut buf).ok_or(BatchError::InvalidVarint)? as usize;
        if buf.remaining() < index.Key.1 {
            return Err(Box::new(BatchError::InvalidKeyLen {
                expected: index.Key.1,
                found: buf.remaining(),
            }));
        }
        // 此刻 cursor所在的位置就是key的数据起始位置
        index.Key.0 = buf.position() as usize;

        // 跳过key的数据
        buf.advance(index.Key.1);

        if index.key_type == keyType::Val {
            if buf.remaining() < 2 {
                return Err(Box::new(BatchError::BatchCorrupted(String::from(
                    "bad record: invalid key length",
                ))));
            }

            // 读取value的长度
            index.Value.1 = GetVarint64Ptr(&mut buf).ok_or(BatchError::InvalidVarint)? as usize;
            if buf.remaining() < index.Value.1 {
                return Err(Box::new(BatchError::InvalidKeyLen {
                    expected: index.Value.1,
                    found: buf.remaining(),
                }));
            }
            // 此刻 cursor所在的位置就是value的起始位置
            index.Value.0 = buf.position() as usize;

            // 跳过value的数据
            buf.advance(index.Value.1);
        } else {
            index.Value = (0, 0);
        }

        // 执行闭包函数
        f(cnt, index)?;
    }
}

// func decodeBatchToMem(data []byte, expectSeq uint64, mdb *memdb.DB) (seq uint64, batchLen int, err error) {
// 	seq, batchLen, err = decodeBatchHeader(data)
// 	if err != nil {
// 		return 0, 0, err
// 	}
// 	if seq < expectSeq {
// 		return 0, 0, newErrBatchCorrupted("invalid sequence number")
// 	}
// 	data = data[batchHeaderLen:]
// 	var ik []byte
// 	var decodedLen int
// 	err = decodeBatch(data, func(i int, index batchIndex) error {
// 		if i >= batchLen {
// 			return newErrBatchCorrupted("invalid records length")
// 		}
// 		ik = makeInternalKey(ik, index.k(data), seq+uint64(i), index.keyType)
// 		if err := mdb.Put(ik, index.v(data)); err != nil {
// 			return err
// 		}
// 		decodedLen++
// 		return nil
// 	})
// 	if err == nil && decodedLen != batchLen {
// 		err = newErrBatchCorrupted(fmt.Sprintf("invalid records length: %d vs %d", batchLen, decodedLen))
// 	}
// 	return
// }
fn decode_batch_to_mem(
    data: &[u8],
    expect_seq: u64,
    mdb: &mut memdb::Db<BytesComparer>,
) -> GenericResult<(u64, u32)> {
    let (seq, batch_len) = decode_batch_header(data)?;
    if seq < expect_seq {
        return Err(Box::new(BatchError::InvalidSeqNum {
            expected: expect_seq,
            found: seq,
        }));
    }

    let data = &data[BATCH_HEADER_LEN as usize..];
    let mut decoded_len = 0;
    decode_batch(data, |i, index| {
        if i as u32 >= batch_len {
            return Err(Box::new(BatchError::InvalidBatchNum {
                expected: batch_len,
                found: i as u32,
            }));
        }

        let ik = makeInternalKey(index.k(data), seq + i as u64, index.key_type);
        mdb.put(&ik, index.v(data))?;

        decoded_len += 1;
        Ok(())
    })?;
    if decoded_len != batch_len {
        return Err(Box::new(BatchError::InvalidBatchNum {
            expected: batch_len,
            found: decoded_len,
        }));
    }

    Ok((seq, batch_len))
}

// func encodeBatchHeader(dst []byte, seq uint64, batchLen int) []byte {
// 	dst = ensureBuffer(dst, batchHeaderLen)
// 	binary.LittleEndian.PutUint64(dst, seq)
// 	binary.LittleEndian.PutUint32(dst[8:], uint32(batchLen))
// 	return dst
// }
fn encocode_batch_header(mut dst: &mut [u8], seq: u64, batch_len: u32) {
    dst.put_u64_le(seq);
    dst.put_u32_le(batch_len);
}
// func decodeBatchHeader(data []byte) (seq uint64, batchLen int, err error) {
// 	if len(data) < batchHeaderLen {
// 		return 0, 0, newErrBatchCorrupted("too short")
// 	}

// 	seq = binary.LittleEndian.Uint64(data)
// 	batchLen = int(binary.LittleEndian.Uint32(data[8:]))
// 	if batchLen < 0 {
// 		return 0, 0, newErrBatchCorrupted("invalid records length")
// 	}
// 	return
// }
fn decode_batch_header(mut data: &[u8]) -> GenericResult<(u64, u32)> {
    if data.len() < BATCH_HEADER_LEN as usize {
        return Err(Box::new(BatchError::BatchCorrupted(String::from(
            "too short",
        ))));
    }

    let mut res = (0, 0);
    res.0 = data.get_u64_le();
    res.1 = data.get_u32_le();
    Ok(res)
}

// func batchesLen(batches []*Batch) int {
// 	batchLen := 0
// 	for _, batch := range batches {
// 		batchLen += batch.Len()
// 	}
// 	return batchLen
// }
fn batches_len(batches: &[&Batch]) -> u32 {
    let mut batch_len = 0;
    for batch in batches {
        batch_len += batch.len()
    }
    batch_len as u32
}

// func writeBatchesWithHeader(wr io.Writer, batches []*Batch, seq uint64) error {
// 	if _, err := wr.Write(encodeBatchHeader(nil, seq, batchesLen(batches))); err != nil {
// 		return err
// 	}
// 	for _, batch := range batches {
// 		if _, err := wr.Write(batch.data); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }
fn write_batches_with_header<T>(wr: &mut T, batches: &[&Batch], seq: u64) -> GenericResult<()>
where
    T: Write,
{
    let mut buf = vec![0; BATCH_HEADER_LEN as usize];
    encocode_batch_header(&mut buf[..], seq, batches_len(batches));
    wr.write_all(&buf[..])?;

    for batch in batches {
        wr.write_all(&batch.data[..])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name() {
        let mut batch = Batch::new();
        batch.append_rec(keyType::Val, &[b'a', b'b'], &[b'k', b'e', b's']);
        batch.append_rec(keyType::Val, &[b'a', b'b'], &vec![0b01; 657890]);
        // assert_eq!(batch.data.len(),16);
        assert_eq!(batch.index.len(), 2);
        let buf = batch.data.to_vec();
        let mut batch2 = Batch::new();
        batch2.decode(&buf[..], 2).unwrap();
    }

    #[test]
    fn test_encode_batch_header() {
        let mut b = vec![0; 12];
        encocode_batch_header(&mut b[..], 1, 2);
        encocode_batch_header(&mut b[..], 1, 2);
        assert_eq!(b, vec![1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0])
    }
}
