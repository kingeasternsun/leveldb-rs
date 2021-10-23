use std::cell::RefCell;
use std::cmp::Ordering;
use std::{sync, vec};

use crate::key::internalKey;
use crate::key::keyType;
use crate::key::makeInternalKey;
use crate::{Comparer, GenericResult};
use bytes::BufMut;
use rand::prelude::*;
const MAX_HEIGHT: usize = 12;
// const (
// 	nKV = iota
// 	nKey
// 	nVal
// 	nHeight
// 	nNext
// )
const NKEY: usize = 1;
const NVAL: usize = 2;
const NHEIGHT: usize = 3;
const NNEXT: usize = 4;

// DB is an in-memory key/value database.
// type DB struct {
// 	cmp comparer.BasicComparer
// 	rnd *rand.Rand
// 	mu     sync.RWMutex
// 	kvData []byte
// 	// Node data:
// 	// [0]         : KV offset
// 	// [1]         : Key length
// 	// [2]         : Value length
// 	// [3]         : Height
// 	// [3..height] : Next nodes
// 	nodeData  []int
// 	prevNode  [tMaxHeight]int
// 	maxHeight int
// 	n         int
// 	kvSize    int
// }
#[allow(non_camel_case_types)]
struct db<T: Comparer> {
    cmp: T,
    // rnd:rand
    // 存储实际key，value的数据
    kv_data: Vec<u8>,
    // 用于记录key,value在kv_data中的索引 ,每一个节点的格式如下 ，其中 level 表示当前节点的层数，后跟 level 个数字，分别表示当前节点的level个层中每一层的下一个节点在node_data中的位置
    // kvOffset1|len(key1)|len(value1)|level|next_node_1|...|next_node_i|...|next_node_h|
    node_data: Vec<usize>, // 前面16个字节对应于 head 节点
    // 在查询过程中，记录搜索过程中所经历的节点(实际是节点在node_data的位置)，主要用于插入和删除
    prev_node: RefCell<[usize; MAX_HEIGHT]>,
    // 当前skiplist的层数
    max_height: usize,
    // skiplist中的节点个数
    n: usize,
    // 数据总大小
    kv_size: usize,
}

impl<T: Comparer> db<T> {
    fn new(cmp: T, capacity: usize) -> Self {
        db {
            cmp: cmp,
            kv_data: Vec::with_capacity(capacity),
            node_data: vec![0; NNEXT + MAX_HEIGHT],
            prev_node: RefCell::new([0; MAX_HEIGHT]),
            max_height: 1,
            n: 0,
            kv_size: 0,
        }
    }

    // func (p *DB) randHeight() (h int) {
    // const branching = 4
    // h = 1
    // for h < tMaxHeight && p.rnd.Int()%branching == 0 {
    // 	h++
    // }
    // return
    // 用于在插入新数据的时候，获取当前的节点的层高
    pub fn rand_heigth(&self) -> usize {
        const branching: usize = 4;
        let mut h = 1;
        while h < MAX_HEIGHT && random::<usize>() % branching == 0 {
            h += 1;
        }
        return h;
    }

    // 计算node节点在level层的下一个节点在node_data中的位置 ,封装一下，提高代码可读性
    fn next_node(&self, node: usize, i: usize) -> usize {
        // + NNEXT 表示在node_data 中,从node位置开始，要跳过  kvOffset1|len(key1)|len(value1)|level| 这4个字节，后面再移动 i 个位置，就到达 next_node_i 了
        self.node_data[node + NNEXT + i]
    }

    fn set_next_node(&mut self, node: usize, i: usize, next: usize) {
        self.node_data[node + NNEXT + i] = next;
    }

    // 根据 node 在 node_data中的位置，求出在kv_data 中的偏移量和长度,从而得到 key
    fn get_key_data(&self, node: usize) -> &[u8] {
        let offset = self.node_data[node];
        &self.kv_data[offset..offset + self.node_data[node + NKEY]]
    }

    // 根据 node 在 node_data 中的位置，求出在kv_data 中的偏移量和长度,从而得到 value
    fn get_value_data(&self, node: usize) -> &[u8] {
        let key_offset = self.node_data[node] + self.node_data[node + NKEY];
        &self.kv_data[key_offset..key_offset + self.node_data[node + NVAL]]
    }
    // Must hold RW-lock if prev == true, as it use shared prevNode slice.
    // func (p *DB) findGE(key []byte, prev bool) (int, bool) {
    // 	node := 0
    // 	h := p.maxHeight - 1
    // 	for {
    // 		next := p.nodeData[node+nNext+h]
    // 		cmp := 1
    // 		if next != 0 {
    // 			o := p.nodeData[next]
    // 			cmp = p.cmp.Compare(p.kvData[o:o+p.nodeData[next+nKey]], key)
    // 		}
    // 		if cmp < 0 {
    // 			// Keep searching in this list
    // 			node = next
    // 		} else {
    // 			if prev {
    // 				p.prevNode[h] = node
    // 			} else if cmp == 0 {
    // 				return next, true
    // 			}
    // 			if h == 0 {
    // 				return next, cmp == 0
    // 			}
    // 			h--
    // 		}
    // 	}
    // }

    // save_pre 标记 在搜索过程中是否要记录遍历过的节点
    pub fn find_great_or_equal(&self, key: &internalKey, save_pre: bool) -> (usize, bool) {
        let mut node = 0;
        // 从高层到底层开始搜索
        let mut i = self.max_height - 1;
        // println!("max_height {}", i);

        loop {
            // 下个节点在 node_data 中的位置
            let next = self.next_node(node, i);
            let mut cmp = Ordering::Greater;
            // 当前链表上没有走到尾
            if next > 0 {
                // 和下个节点next进行key比较
                cmp = self.cmp.compare(self.get_key_data(next), key.data());
            }

            // 大于下一个节点，继续沿着当前层 向右 跳
            if cmp == Ordering::Less {
                node = next;
            } else {
                // 小于等于下一个节点 或 下一个节点是空
                // if save_pre {
                //     // 对于插入或删除而进行的搜索，即使遇到相同的也要继续往下一层比较,不能立即返回
                //     // 所以这里要先于 cmp == Ordering::Equal 的判断
                //     self.prev_node.borrow_mut()[i] = node;
                // } else if cmp == Ordering::Equal {
                //     // find_great_or_equal 跟 find_less 的一个不同就是这里返回的是 next
                //     return (next, true);
                // }

                // 改成下面的方式可读性更高
                if (!save_pre) && cmp == Ordering::Equal {
                    return (next, true);
                }
                if save_pre {
                    self.prev_node.borrow_mut()[i] = node;
                }

                if i == 0 {
                    return (next, cmp == Ordering::Equal);
                }

                i -= 1;
            }
        }
    }

    // func (p *DB) findLT(key []byte) int {
    // 	node := 0
    // 	h := p.maxHeight - 1
    // 	for {
    // 		next := p.nodeData[node+nNext+h]
    // 		o := p.nodeData[next]
    // 		if next == 0 || p.cmp.Compare(p.kvData[o:o+p.nodeData[next+nKey]], key) >= 0 {
    // 			if h == 0 {
    // 				break
    // 			}
    // 			h--
    // 		} else {
    // 			node = next
    // 		}
    // 	}
    // 	return node
    // }

    // 查询小于 key 的，离key最近的节点。这个不涉及保存pre_node 逻辑简单
    fn find_lessthan(&self, key: &internalKey) -> usize {
        let mut node = 0;
        let mut i = self.max_height - 1;
        loop {
            let next = self.next_node(node, i);
            // 当前链表到尾部了 或 下个节点的key 大于等于 key， 向下一层
            if next == 0 || self.cmp.compare(self.get_key_data(next), key.data()) != Ordering::Less
            {
                if i == 0 {
                    // 这里是返回 node 而不是 next
                    return node;
                }
                i -= 1;
            } else {
                // 当前链表 跳到 下一个节点
                node = next;
            }
        }
    }

    // func (p *DB) findLast() int {
    // 	node := 0
    // 	h := p.maxHeight - 1
    // 	for {
    // 		next := p.nodeData[node+nNext+h]
    // 		if next == 0 {
    // 			if h == 0 {
    // 				break
    // 			}
    // 			h--
    // 		} else {
    // 			node = next
    // 		}
    // 	}
    // 	return node
    // }

    // 查询最后一个 ,逻辑也简单，每一层沿着链表走到尾部，然后跳到下一层，再沿着链表走到底
    pub fn find_last(&self) -> usize {
        let mut node = 0;
        let mut i = self.max_height - 1;
        loop {
            let next = self.next_node(node, i);
            // 走到了尾部，
            if next == 0 {
                if i == 0 {
                    return node;
                }
                // 跳过下一层
                i -= 1;
            } else {
                // 沿着链表往后面跳
                node = next;
            }
        }
    }
}

pub struct Db<T: Comparer> {
    db: sync::RwLock<db<T>>,
}

impl<T: Comparer> Db<T> {
    // Put sets the value for the given key. It overwrites any previous value
    // for that key; a DB is not a multi-map.
    //
    // It is safe to modify the contents of the arguments after Put returns.
    // func (p *DB) Put(key []byte, value []byte) error {
    // 	p.mu.Lock()
    // 	defer p.mu.Unlock()

    // 	if node, exact := p.findGE(key, true); exact {
    // 		kvOffset := len(p.kvData)
    // 		p.kvData = append(p.kvData, key...)
    // 		p.kvData = append(p.kvData, value...)
    // 		p.nodeData[node] = kvOffset
    // 		m := p.nodeData[node+nVal]
    // 		p.nodeData[node+nVal] = len(value)
    // 		p.kvSize += len(value) - m
    // 		return nil
    // 	}

    // 	h := p.randHeight()
    // 	if h > p.maxHeight {
    // 		for i := p.maxHeight; i < h; i++ {
    // 			p.prevNode[i] = 0
    // 		}
    // 		p.maxHeight = h
    // 	}

    // 	kvOffset := len(p.kvData)
    // 	p.kvData = append(p.kvData, key...)
    // 	p.kvData = append(p.kvData, value...)
    // 	// Node
    // 	node := len(p.nodeData)
    // 	p.nodeData = append(p.nodeData, kvOffset, len(key), len(value), h)
    // 	for i, n := range p.prevNode[:h] {
    // 		m := n + nNext + i
    // 		p.nodeData = append(p.nodeData, p.nodeData[m])
    // 		p.nodeData[m] = node
    // 	}

    // 	p.kvSize += len(key) + len(value)
    // 	p.n++
    // 	return nil
    // }
    pub fn put(&mut self, key: &internalKey, value: Option<&[u8]>) -> GenericResult<()> {
        let mut db = self.db.write().unwrap();
        let (node, exist) = db.find_great_or_equal(key, true);
        if exist {
            // TODO 优化，如果新value的长度小于等于旧的value，直接覆盖直接的，不用重新分配
            // 如果key已经存在，直接覆盖之前的value
            // 数据是追加的方式，所以当前kv_data的长度就是node节点的在 skv_data 上的新的偏移量
            let offset = db.kv_data.len();
            // 追加 key 和 value 的数据
            db.kv_data.append(&mut key.data().to_vec());
            let mut v_len = 0;
            if let Some(value) = value {
                v_len = value.len();
                db.kv_data.append(&mut value.to_vec());
            }
            // 更新node的偏移量
            db.node_data[node] = offset;
            // value 的长度可能也变化了
            // 之前的长度
            let v_old = db.node_data[node + NVAL];
            // 更新为新的长度
            db.node_data[node + NVAL] = v_len;
            // 更新数据总大小
            db.kv_size += v_len - v_old;
            return Ok(());
        }

        let h = db.rand_heigth();
        // 处理head节点
        if h > db.max_height {
            for i in db.max_height..h + 1 {
                db.prev_node.borrow_mut()[i] = 0;
            }
            db.max_height = h;
            println!("height {}", h);
        }

        // 新增节点在 kv_data 中的起始偏移
        let kv_offset = db.kv_data.len();
        // 追加 key 和 value 的数据
        db.kv_data.append(&mut key.data().to_vec());
        let mut v_len = 0;
        if let Some(value) = value {
            v_len = value.len();
            db.kv_data.append(&mut value.to_vec());
        }

        // 创建新节点,因为是追加方式，所以当前 node_data 的长度 就是新节点在 node_data 的位置
        let node = db.node_data.len();
        // 添加新节点
        db.node_data.push(kv_offset);
        db.node_data.push(key.data().len());
        db.node_data.push(v_len);
        db.node_data.push(h);

        // 这里出问题了
        // for (i,n )in db.prev_node.borrow()[0..h].iter().enumerate(){
        //     let next = db.next_node(*n, i);
        //     db.node_data.push(next);

        // }
        // error[E0502]: cannot borrow `db` as mutable because it is also borrowed as immutable
        //    --> src/memdb/memdb.rs:343:13
        //     |
        // 341 |         for (i,n )in db.prev_node.borrow()[0..h].iter().enumerate(){
        //     |                      ---------------------
        //     |                      |
        //     |                      immutable borrow occurs here
        //     |                      a temporary with access to the immutable borrow is created here ...
        // 342 |             let next = db.next_node(*n, i);
        // 343 |             db.node_data.push(next);
        //     |             ^^ mutable borrow occurs here
        // 344 |
        // 345 |         }
        // 再添加n个元素用来保存节点在h层中每一层的下个节点的位置
        db.node_data.resize(node + NNEXT + h, 0);
        // let pre_node =  db.prev_node.borrow()[0..h].to_vec();
        // for (i,n)in pre_node.iter().enumerate(){
        //     let next = db.next_node(*n, i);
        //     db.set_next_node(node, i, next);
        //     db.set_next_node(*n, i, node);
        // }
        for i in 0..h {
            let n = db.prev_node.borrow()[i];
            let next = db.next_node(n, i);
            db.set_next_node(node, i, next);
            db.set_next_node(n, i, node);
        }

        db.kv_size += key.data().len() + v_len;
        db.n += 1;

        Ok(())
    }

    pub fn delete(&mut self, key: &internalKey) -> Option<()> {
        let mut db = self.db.write().unwrap();
        let (node, exist) = db.find_great_or_equal(key, true);
        if !exist {
            return None;
        }

        // 当前节点有几层
        let h = db.node_data[node + NHEIGHT];
        // 开始删除, 让前一个节点指向前一个节点的下一个节点的下一个节点 pre->next = pre->next->next
        for i in 0..h {
            let pre = db.prev_node.borrow()[i];
            // let pre_next = db.next_node(pre, i);
            // if pre_next != node {
            //     print!("{}:{}", pre_next, node);
            // }

            // let next_next = db.next_node(pre_next, i);
            // db.set_next_node(pre, i, next_next);

            // 由于 前一个节点的下一个节点 pre_node 就是当前节点 node ，所以上面代码可以优化为
            let next_next = db.next_node(node, i);
            db.set_next_node(pre, i, next_next);
        }

        db.kv_size -= db.node_data[node + NKEY] + db.node_data[node + NVAL];
        db.n -= 1;

        Some(())
    }

    pub fn contains(&self, key: &internalKey) -> bool {
        let db = self.db.read().unwrap();
        let (_, exist) = db.find_great_or_equal(key, false);
        return exist;
    }
    // Get gets the value for the given key. It returns error.ErrNotFound if the
    // DB does not contain the key.
    //
    // The caller should not modify the contents of the returned slice, but
    // it is safe to modify the contents of the argument after Get returns.
    // func (p *DB) Get(key []byte) (value []byte, err error) {
    // 	p.mu.RLock()
    // 	if node, exact := p.findGE(key, false); exact {
    // 		o := p.nodeData[node] + p.nodeData[node+nKey]
    // 		value = p.kvData[o : o+p.nodeData[node+nVal]]
    // 	} else {
    // 		err = ErrNotFound
    // 	}
    // 	p.mu.RUnlock()
    // 	return
    // }

    pub fn get(&self, key: &internalKey) -> Option<Vec<u8>> {
        let db = self.db.read().unwrap();
        let (node, exist) = db.find_great_or_equal(key, false);
        if exist {
            Some(db.get_value_data(node).to_vec())
        } else {
            None
        }
    }

    // Find finds key/value pair whose key is greater than or equal to the
    // given key. It returns ErrNotFound if the table doesn't contain
    // such pair.
    //
    // The caller should not modify the contents of the returned slice, but
    // it is safe to modify the contents of the argument after Find returns.
    // func (p *DB) Find(key []byte) (rkey, value []byte, err error) {
    // 	p.mu.RLock()
    // 	if node, _ := p.findGE(key, false); node != 0 {
    // 		n := p.nodeData[node]
    // 		m := n + p.nodeData[node+nKey]
    // 		rkey = p.kvData[n:m]
    // 		value = p.kvData[m : m+p.nodeData[node+nVal]]
    // 	} else {
    // 		err = ErrNotFound
    // 	}
    // 	p.mu.RUnlock()
    // 	return
    // }

    fn find(&self, key: &internalKey) -> Option<(Vec<u8>, Vec<u8>)> {
        let db = self.db.read().unwrap();
        let (node, exist) = db.find_great_or_equal(key, false);
        if exist {
            Some((
                db.get_key_data(node).to_vec(),
                db.get_value_data(node).to_vec(),
            ))
        } else {
            None
        }
    }

    fn capacity(&self) -> usize {
        self.db.read().unwrap().kv_data.capacity()
    }

    fn size(&self) -> usize {
        self.db.read().unwrap().kv_size
    }

    fn free(&self) -> usize {
        let db = self.db.read().unwrap();
        db.kv_data.capacity() - db.kv_data.len()
    }

    fn len(&self) -> usize {
        self.db.read().unwrap().n
    }
    // Reset resets the DB to initial empty state. Allows reuse the buffer.
    // func (p *DB) Reset() {
    // 	p.mu.Lock()
    // 	p.rnd = rand.New(rand.NewSource(0xdeadbeef))
    // 	p.maxHeight = 1
    // 	p.n = 0
    // 	p.kvSize = 0
    // 	p.kvData = p.kvData[:0]
    // 	p.nodeData = p.nodeData[:nNext+tMaxHeight]
    // 	p.nodeData[nKV] = 0
    // 	p.nodeData[nKey] = 0
    // 	p.nodeData[nVal] = 0
    // 	p.nodeData[nHeight] = tMaxHeight
    // 	for n := 0; n < tMaxHeight; n++ {
    // 		p.nodeData[nNext+n] = 0
    // 		p.prevNode[n] = 0
    // 	}
    // 	p.mu.Unlock()
    // }
    fn reset(&mut self) {
        let mut db = self.db.write().unwrap();
        db.max_height = 1;
        db.n = 0;
        db.kv_size = 0;
        db.kv_data.clear();
        db.node_data.resize(NNEXT + MAX_HEIGHT, 0);
        db.prev_node.borrow_mut().fill(0);
    }

    fn new(cmp: T, capacity: usize) -> Self {
        Db {
            db: sync::RwLock::new(db::new(cmp, capacity)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::BytesComparer;

    use super::*;

    #[test]
    fn test_name() {
        let mut db = Db::new(BytesComparer {}, 100);
        let max_iter_num = 1000000;
        for i in 1..max_iter_num {
            // println!("to add {}",i);
            let mut data = vec![];
            // let i = i*100;
            data.put_u32_le(i);
            let key = makeInternalKey(&data[0..4], 10, keyType::Val);
            db.put(&key, Some(&data[0..4])).unwrap();
            db.get(&key).unwrap();
        }

        for i in 1..max_iter_num {
            // println!("to add {}",i);
            let mut data = vec![];
            // let i = i*100;
            data.put_u32_le(i);
            let key = makeInternalKey(&data[0..4], 10, keyType::Val);
            db.delete(&key).unwrap();
        }
    }
}
