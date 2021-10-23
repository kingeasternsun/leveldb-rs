use crate::compare::basic_comparer;
use crate::compare::Comparer;
use std::cmp::Ordering;
// type bytesComparer struct{}
pub struct BytesComparer;

// func (bytesComparer) Compare(a, b []byte) int {
// 	return bytes.Compare(a, b)
// }

impl basic_comparer for BytesComparer {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
}

impl Comparer for BytesComparer {
    // func (bytesComparer) Name() string {
    // 	return "leveldb.BytewiseComparator"
    // }
    fn name(&self) -> String {
        todo!()
    }

    // func (bytesComparer) Separator(dst, a, b []byte) []byte {
    // 	i, n := 0, len(a)
    // 	if n > len(b) {
    // 		n = len(b)
    // 	}
    // 	for ; i < n && a[i] == b[i]; i++ {
    // 	}
    // 	if i >= n {
    // 		// Do not shorten if one string is a prefix of the other
    // 	} else if c := a[i]; c < 0xff && c+1 < b[i] {
    // 		dst = append(dst, a[:i+1]...)
    // 		dst[len(dst)-1]++
    // 		return dst
    // 	}
    // 	return nil
    // }
    fn separator<'a>(&self, dst: &'a [u8], a: &'a [u8], b: &'a [u8]) -> &'a [u8] {
        todo!()
    }

    // func (bytesComparer) Successor(dst, b []byte) []byte {
    // 	for i, c := range b {
    // 		if c != 0xff {
    // 			dst = append(dst, b[:i+1]...)
    // 			dst[len(dst)-1]++
    // 			return dst
    // 		}
    // 	}
    // 	return nil
    // }
    fn successor<'a>(&self, dst: &'a [u8], b: &'a [u8]) -> &'a [u8] {
        todo!()
    }
}

// // DefaultComparer are default implementation of the Comparer interface.
// // It uses the natural ordering, consistent with bytes.Compare.
// var DefaultComparer = bytesComparer{}
