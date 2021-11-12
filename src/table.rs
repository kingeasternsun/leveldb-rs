#[allow(non_camel_case_types)]
#[allow(dead_code)]
use crate::compare::Comparer;
use crate::icompare::IComparer;
use crate::key::{internalKey, keyType, makeInternalKey, KEY_MAX_SEQ};
use crate::storage;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::atomic;
use std::sync::atomic::Ordering;
use std::sync::Arc;
// tFile holds basic information about a table.
// type tFile struct {
// 	fd         storage.FileDesc
// 	seekLeft   int32
// 	size       int64
// 	imin, imax internalKey
// }
#[derive(Clone)]
pub struct tFile {
    fd: storage::FileDesc,
    seek_left: Arc<atomic::AtomicI32>,
    size: i64,
    imin: internalKey,
    imax: internalKey,
}

impl tFile {
    // Returns true if given key is after largest key of this table.
    // func (t *tFile) after(icmp *iComparer, ukey []byte) bool {
    // 	return ukey != nil && icmp.uCompare(ukey, t.imax.ukey()) > 0
    // }
    pub fn after<T>(&self, icmp: &IComparer<T>, ukey: &[u8]) -> bool
    where
        T: Comparer,
    {
        return ukey.len() > 0 && icmp.u_compare(ukey, self.imax.ukey()) == Greater;
    }

    // Returns true if given key is before smallest key of this table.
    // func (t *tFile) before(icmp *iComparer, ukey []byte) bool {
    // 	return ukey != nil && icmp.uCompare(ukey, t.imin.ukey()) < 0
    // }
    pub fn before<T>(&self, icmp: &IComparer<T>, ukey: &[u8]) -> bool
    where
        T: Comparer,
    {
        return ukey.len() > 0 && icmp.u_compare(ukey, self.imin.ukey()) > Greater;
    }

    // Returns true if given key range overlaps with this table key range.
    // func (t *tFile) overlaps(icmp *iComparer, umin, umax []byte) bool {
    // 	return !t.after(icmp, umin) && !t.before(icmp, umax)
    // }
    pub fn overlaps<T>(&self, icmp: &IComparer<T>, umin: &[u8], umax: &[u8]) -> bool
    where
        T: Comparer,
    {
        return !self.after(icmp, umin) && !self.before(icmp, umax);
    }

    // Cosumes one seek and return current seeks left.
    // func (t *tFile) consumeSeek() int32 {
    // 	return atomic.AddInt32(&t.seekLeft, -1)
    // }
    pub fn consume_seek(&self) -> i32 {
        self.seek_left.fetch_sub(1, Ordering::Relaxed)
    }
}

/*
// Creates new tFile.
func newTableFile(fd storage.FileDesc, size int64, imin, imax internalKey) *tFile {
    f := &tFile{
        fd:   fd,
        size: size,
        imin: imin,
        imax: imax,
    }

    // We arrange to automatically compact this file after
    // a certain number of seeks.  Let's assume:
    //   (1) One seek costs 10ms
    //   (2) Writing or reading 1MB costs 10ms (100MB/s)
    //   (3) A compaction of 1MB does 25MB of IO:
    //         1MB read from this level
    //         10-12MB read from next level (boundaries may be misaligned)
    //         10-12MB written to next level
    // This implies that 25 seeks cost the same as the compaction
    // of 1MB of data.  I.e., one seek costs approximately the
    // same as the compaction of 40KB of data.  We are a little
    // conservative and allow approximately one seek for every 16KB
    // of data before triggering a compaction.
    f.seekLeft = int32(size / 16384)
    if f.seekLeft < 100 {
        f.seekLeft = 100
    }

    return f
}

func tableFileFromRecord(r atRecord) *tFile {
    return newTableFile(storage.FileDesc{Type: storage.TypeTable, Num: r.num}, r.size, r.imin, r.imax)
}
*/

// tFiles hold multiple tFile.
pub struct tFiles(Vec<tFile>);
#[allow(dead_code)]
impl tFiles {
    // func (tf tFiles) nums() string {
    // 	x := "[ "
    // 	for i, f := range tf {
    // 		if i != 0 {
    // 			x += ", "
    // 		}
    // 		x += fmt.Sprint(f.fd.Num)
    // 	}
    // 	x += " ]"
    // 	return x
    // }

    // 虽然 let s3 = s1 + &s2; 看起来就像它会复制两个字符串并创建一个新的字符串，而实际上这个语句会获取 s1 的所有权，附加上从 s2 中拷贝的内容，并返回结果的所有权。
    // 换句话说，它看起来好像生成了很多拷贝，不过实际上并没有：这个实现比拷贝要更高效。
    pub fn nums(&self) -> String {
        let mut x = String::from("[ ");
        /*
        for (i,f) in self.0.iter().enumerate(){
            if i !=0{
                x.push_str(", ");
            }
            x.push_str(&format!("{}",f.fd.num));
        }
        x.push_str(" ]");

        */
        // 直接使用 +
        for (i, f) in self.0.iter().enumerate() {
            if i != 0 {
                x += ", ";
            }
            x += &format!("{}", f.fd.num);
        }
        x += " ]";
        x
    }

    // Returns true if i smallest key is less than j.
    // This used for sort by key in ascending order.
    // func (tf tFiles) lessByKey(icmp *iComparer, i, j int) bool {
    // 	a, b := tf[i], tf[j]
    // 	n := icmp.Compare(a.imin, b.imin)
    // 	if n == 0 {
    // 		return a.fd.Num < b.fd.Num
    // 	}
    // 	return n < 0
    // }
    // 如果第i个tFile的最小值 小于 第j个tFile的最小值，返回true，用于递增排序,
    // 这个方法可以去掉, 直接使用 sort_unstable_by 进行排序
    pub fn less_by_key<T: Comparer>(
        &self,
        icmp: &IComparer<T>,
        i: usize,
        j: usize,
    ) -> std::cmp::Ordering {
        let n = icmp.compare(self.0[i].imin.data(), self.0[j].imin.data());
        if n == Equal {
            return self.0[i].fd.num.cmp(&self.0[j].fd.num);
        }
        n
    }
    // Returns true if i file number is greater than j.
    // This used for sort by file number in descending order.
    // func (tf tFiles) lessByNum(i, j int) bool {
    // 	return tf[i].fd.Num > tf[j].fd.Num
    // }

    // 如果第i个文件的file number 大于第j个文件的file number，返回true;
    // 用于基于 file number 递减排序, 这个方法可以去掉
    pub fn less_by_num(&self, i: usize, j: usize) -> bool {
        return self.0[i].fd.num < self.0[j].fd.num;
    }

    // Sorts tables by key in ascending order.
    // func (tf tFiles) sortByKey(icmp *iComparer) {
    // 	sort.Sort(&tFilesSortByKey{tFiles: tf, icmp: icmp})
    // }
    pub fn sort_by_key<T: Comparer>(&mut self, icmp: &IComparer<T>) {
        self.0.sort_unstable_by(|a, b| less_by_key(icmp, a, b));
    }
    // Sorts tables by file number in descending order.
    // func (tf tFiles) sortByNum() {
    // 	sort.Sort(&tFilesSortByNum{tFiles: tf})
    // }
    // 基于 file number 递减排序
    pub fn sort_by_num(&mut self) {
        self.0.sort_unstable_by(|a, b| a.fd.num.cmp(&b.fd.num));
    }

    // Returns sum of all tables size.
    // func (tf tFiles) size() (sum int64) {
    // 	for _, t := range tf {
    // 		sum += t.size
    // 	}
    // 	return sum
    // }

    pub fn size(&self) -> i64 {
        let mut sum = 0;
        for t in &self.0 {
            sum += t.size;
        }
        sum
    }

    // Searches smallest index of tables whose its smallest
    // key is after or equal with given key.
    // func (tf tFiles) searchMin(icmp *iComparer, ikey internalKey) int {
    // 	return sort.Search(len(tf), func(i int) bool {
    // 		return icmp.Compare(tf[i].imin, ikey) >= 0
    // 	})
    // }
    // 搜索 index 最小的，且最小值imin 大于或等于特定值 ikey 的 tFile 的索引 tfile.imin>=ikey
    pub fn search_min<T: Comparer>(&mut self, icmp: &IComparer<T>, ikey: internalKey) -> usize {
        // 也就是寻找分割点 左边的是 tfile.imin < ikey，右边的是 tfile.imin >= ikey
        self.0
            .partition_point(|f| icmp.compare(f.imin.data(), ikey.data()) == Less)
    }

    // Searches smallest index of tables whose its largest
    // key is after or equal with given key.
    // func (tf tFiles) searchMax(icmp *iComparer, ikey internalKey) int {
    // 	return sort.Search(len(tf), func(i int) bool {
    // 		return icmp.Compare(tf[i].imax, ikey) >= 0
    // 	})
    // }
    // 搜索 index 最小的，且最大值imax 大于或等于特定值 ikey 的 tFile 的索引, tfile.imax>=ikey
    pub fn search_max<T: Comparer>(&mut self, icmp: &IComparer<T>, ikey: internalKey) -> usize {
        // 也就是寻找分割点 左边的是 tfile.imax < ikey，右边的是 tfile.imax >= ikey
        self.0
            .partition_point(|f| icmp.compare(f.imax.data(), ikey.data()) == Less)
    }

    // Searches smallest index of tables whose its file number
    // is smaller than the given number.
    // func (tf tFiles) searchNumLess(num int64) int {
    // 	return sort.Search(len(tf), func(i int) bool {
    // 		return tf[i].fd.Num < num
    // 	})
    // }
    // 搜索 index 最小的，且 tfile.fd.num < num
    pub fn search_num_less(&mut self, num: i64) -> usize {
        // 也就是寻找分割点 左边的是 tfile.fd.num >= num
        self.0.partition_point(|f| f.fd.num >= num)
    }

    // Searches smallest index of tables whose its smallest
    // key is after the given key.
    // func (tf tFiles) searchMinUkey(icmp *iComparer, umin []byte) int {
    // 	return sort.Search(len(tf), func(i int) bool {
    // 		return icmp.ucmp.Compare(tf[i].imin.ukey(), umin) > 0
    // 	})
    // }
    // 寻找 index 最小的tFile, tfile.imin.ukey()大于 ukey
    pub fn search_min_ukey<T: Comparer>(&self, icmp: &IComparer<T>, ukey: &[u8]) -> usize {
        // 也就是寻找分割点 左边区域的是 tfile.imin.ukey() <= ukey
        self.0
            .partition_point(|f| icmp.u_compare(f.imin.ukey(), ukey) != Greater)
    }

    // Searches smallest index of tables whose its largest
    // key is after the given key.
    // func (tf tFiles) searchMaxUkey(icmp *iComparer, umax []byte) int {
    // 	return sort.Search(len(tf), func(i int) bool {
    // 		return icmp.ucmp.Compare(tf[i].imax.ukey(), umax) > 0
    // 	})
    // }
    // 寻找 index 最小的tFile, tfile.imax.ukey()大于 ukey
    // 
    pub fn search_max_ukey<T: Comparer>(&self, icmp: &IComparer<T>, ukey: &[u8]) -> usize {
        // 也就是寻找分割点 左边区域的是 tfile.imax.ukey() <= ukey
        self.0
            .partition_point(|f| icmp.u_compare(f.imax.ukey(), ukey) != Greater)
    }

    // Returns true if given key range overlaps with one or more
    // tables key range. If unsorted is true then binary search will not be used.
    // func (tf tFiles) overlaps(icmp *iComparer, umin, umax []byte, unsorted bool) bool {
    // 	if unsorted {
    // 		// Check against all files.
    // 		for _, t := range tf {
    // 			if t.overlaps(icmp, umin, umax) {
    // 				return true
    // 			}
    // 		}
    // 		return false
    // 	}

    // 	i := 0
    // 	if len(umin) > 0 {
    // 		// Find the earliest possible internal key for min.
    // 		i = tf.searchMax(icmp, makeInternalKey(nil, umin, keyMaxSeq, keyTypeSeek))
    // 	}
    // 	if i >= len(tf) {
    // 		// Beginning of range is after all files, so no overlap.
    // 		return false
    // 	}
    // 	return !tf[i].before(icmp, umax)
    // }
    // 判断区间 [umin,umax]是否有和其中某个tFile的区间有重叠
    fn overlaps<T: Comparer>(
        &mut self,
        icmp: &IComparer<T>,
        umin: &[u8],
        umax: &[u8],
        unsorted: bool,
    ) -> bool {
        if unsorted {
            for t in &self.0 {
                if t.overlaps(icmp, umin, umax) {
                    return true;
                }
            }
            return false;
        }

        let mut i = 0;
        if umin.len() > 0 {
            i = self.search_max(icmp, makeInternalKey(umin, KEY_MAX_SEQ, keyType::Seek));
        }
        if i >= self.0.len() {
            return false;
        }
        return !self.0[i].before(icmp, umax);
    }

    // Returns tables whose its key range overlaps with given key range.
    // Range will be expanded if ukey found hop across tables.
    // If overlapped is true then the search will be restarted if umax
    // expanded.
    // The dst content will be overwritten.
    // func (tf tFiles) getOverlaps(dst tFiles, icmp *iComparer, umin, umax []byte, overlapped bool) tFiles {
    // 	// Short circuit if tf is empty
    // 	if len(tf) == 0 {
    // 		return nil
    // 	}
    // 	// For non-zero levels, there is no ukey hop across at all.
    // 	// And what's more, the files in these levels are strictly sorted,
    // 	// so use binary search instead of heavy traverse.
    // 	if !overlapped {
    // 		var begin, end int
    // 		// Determine the begin index of the overlapped file
    // 		if umin != nil {
    // 			index := tf.searchMinUkey(icmp, umin)
    // 			if index == 0 {
    // 				begin = 0
    // 			} else if bytes.Compare(tf[index-1].imax.ukey(), umin) >= 0 {
    // 				// The min ukey overlaps with the index-1 file, expand it.
    // 				begin = index - 1
    // 			} else {
    // 				begin = index
    // 			}
    // 		}
    // 		// Determine the end index of the overlapped file
    // 		if umax != nil {
    // 			index := tf.searchMaxUkey(icmp, umax)
    // 			if index == len(tf) {
    // 				end = len(tf)
    // 			} else if bytes.Compare(tf[index].imin.ukey(), umax) <= 0 {
    // 				// The max ukey overlaps with the index file, expand it.
    // 				end = index + 1
    // 			} else {
    // 				end = index
    // 			}
    // 		} else {
    // 			end = len(tf)
    // 		}
    // 		// Ensure the overlapped file indexes are valid.
    // 		if begin >= end {
    // 			return nil
    // 		}
    // 		dst = make([]*tFile, end-begin)
    // 		copy(dst, tf[begin:end])
    // 		return dst
    // 	}

    // 	dst = dst[:0]
    // 	for i := 0; i < len(tf); {
    // 		t := tf[i]
    // 		if t.overlaps(icmp, umin, umax) {
    // 			if umin != nil && icmp.uCompare(t.imin.ukey(), umin) < 0 {
    // 				umin = t.imin.ukey()
    // 				dst = dst[:0]
    // 				i = 0
    // 				continue
    // 			} else if umax != nil && icmp.uCompare(t.imax.ukey(), umax) > 0 {
    // 				umax = t.imax.ukey()
    // 				// Restart search if it is overlapped.
    // 				dst = dst[:0]
    // 				i = 0
    // 				continue
    // 			}

    // 			dst = append(dst, t)
    // 		}
    // 		i++
    // 	}

    // 	return dst
    // }

    // 返回和给定key区间有重叠的tFile列表
    //
    fn get_overlaps<T: Comparer>(
        &self,
        icmp: &IComparer<T>,
        umin: &[u8],
        umax: &[u8],
        overlapped: bool,
    ) -> Self {
        let mut umin = umin;
        let mut umax = umax;
        let mut res = tFiles(vec![]);
        if self.0.len() == 0 {
            return res;
        }
        // 对于非0 层，ukey没有hop，这些文件都是严格排序的，
        if !overlapped {
            let mut begin = 0;
            let mut end = 0;
            if umin.len() > 0 {
                // 首先搜索最左边的，且文件的最小值大于umin的文件
                let index = self.search_min_ukey(icmp, &umin);
                if index == 0 {
                    begin = index;
                    // 如果 前一个文件的最大值大于等于umin,说明前一个文件 和区间 [umin,umax]重叠
                } else if icmp.u_compare(self.0[index - 1].imax.ukey(), umin) != Less {
                    begin = index - 1;
                } else {
                    begin = index;
                }
            }
            if umin.len() > 0 {
                // 首先搜索最左边的，且文件的最大值大于umax的文件
                let index = self.search_max_ukey(icmp, &umax);

                if index == self.0.len() {
                    end = self.0.len();
                    // 如果当前文件的最小值小于等于umax, 说明这个文件和区间 [umin,umax]重叠
                } else if icmp.u_compare(self.0[index].imin.ukey(), umax) != Greater {
                    end = index + 1;
                } else {
                    end = index;
                }
            } else {
                end = self.0.len();
            }

            if begin >= end {
                return res;
            }

            res.0.clone_from_slice(&self.0[begin..end]);
        }

        let mut i = 0;
        while i < self.0.len() {
            // 如果和区间重叠，则把两个区间合并，更新 umin umax
            if self.0[i].overlaps(icmp, umin, umax) {
                // 如果当前文件的最小值比 umin 还要小，更新umin
                if umin.len() > 0 && icmp.u_compare(self.0[i].imin.ukey(), umin) == Less {
                    umin = self.0[i].imin.ukey();
                    res.0.clear();
                    i = 0;
                    continue;
                    // 如果当前文件的最大值比 umax 还要大，更新umax
                } else if umax.len() > 0 && icmp.u_compare(self.0[i].imax.ukey(), umax) == Greater {
                    umax = self.0[i].imax.ukey();
                    res.0.clear();
                    i = 0;
                    continue;
                }
                res.0.push(self.0[i].clone());
            }
            i += 1;
        }

        res
    }

    // Returns tables key range.
    // func (tf tFiles) getRange(icmp *iComparer) (imin, imax internalKey) {
    // 	for i, t := range tf {
    // 		if i == 0 {
    // 			imin, imax = t.imin, t.imax
    // 			continue
    // 		}
    // 		if icmp.Compare(t.imin, imin) < 0 {
    // 			imin = t.imin
    // 		}
    // 		if icmp.Compare(t.imax, imax) > 0 {
    // 			imax = t.imax
    // 		}
    // 	}

    // 	return
    // }
    fn get_range<T: Comparer>(&self, icmp: &IComparer<T>) -> (internalKey, internalKey) {
        // res.0 记录最小值的索引， res.1 记录最大值的索引
        let mut res = (0, 0);
        for (i, t) in self.0.iter().enumerate() {
            if icmp.compare(t.imin.data(), self.0[res.0].imin.data()) == Less {
                res.0 = i;
            }

            if icmp.compare(t.imax.data(), self.0[res.1].imax.data()) == Greater {
                res.1 = i
            }
        }
        (self.0[res.0].imin.clone(), self.0[res.1].imax.clone())
    }
}

pub fn less_by_key<T: Comparer>(icmp: &IComparer<T>, a: &tFile, b: &tFile) -> std::cmp::Ordering {
    let n = icmp.compare(a.imin.data(), b.imin.data());
    if n == Equal {
        return a.fd.num.cmp(&b.fd.num);
    }
    n
}
