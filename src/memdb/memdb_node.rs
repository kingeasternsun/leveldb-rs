use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;
use std::sync::Arc;
use std::{sync, vec};

use crate::key::internalKey;
use crate::key::keyType;
use crate::key::makeInternalKey;
use crate::{Comparer, GenericResult};
use bytes::BufMut;
use rand::prelude::*;
const MAX_HEIGHT: usize = 12;

#[allow(non_camel_case_types)]
struct db_skip<T: Comparer> {
    cmp: T,
    // 存储实际key，value的数据 ，offset从1开始，  offset为0的表示head节点
    kv_data: Vec<u8>,
    head: RcNode, // 头部，
    // 当前skiplist的层数
    max_height: usize,
    // skiplist中的节点个数
    n: usize,
    // 数据总大小
    kv_size: usize,
}

impl<T: Comparer> db_skip<T> {

    fn new(cmp:T)->Self{
        let head = Rc::new(RefCell::new(node::new(0, 0, 0, 1)));
        Self{
            cmp:cmp,
            kv_data:vec![],
            max_height:1,
            head:head,
            n:0,
            kv_size:0,
        }
    }
    // 根据 node 在 node_data中的位置，求出在kv_data 中的偏移量和长度,从而得到 key
    fn get_key_data(&self, node: &node) -> &[u8] {
        &self.kv_data[node.offset..node.offset + node.key_len]
    }

    // 根据 node 在 node_data 中的位置，求出在kv_data 中的偏移量和长度,从而得到 value
    fn get_value_data(&self, node: &node) -> &[u8] {
        &self.kv_data[node.offset + node.key_len..node.offset + node.key_len + node.value_len]
    }

    // 用于在插入新数据的时候，获取当前的节点的层高
    pub fn rand_heigth(&self) -> usize {
        const branching: usize = 4;
        let mut h = 1;
        while h < MAX_HEIGHT && random::<usize>() % branching == 0 {
            h += 1;
        }
        return h;
    }

    pub fn find_great_or_equal(
        &self,
        key: &internalKey,
        pre_node: &mut Option<Vec<RcNode>>, // 注意 不能用 Option<&mut Vec<RcNode>>
    ) -> (RcNode, bool) {
        let mut node = Rc::clone(&self.head); // 从头节点开始
        let mut next_node = Rc::clone(&node);
        let mut i = self.max_height - 1;

        loop {
            // 这里将 cmp 预先设置为 Ordering::Less 是一个非常巧妙的方式, 就可以自动包含 下个节点为空(当作是无穷大)的情况了
            let mut cmp = Ordering::Less;
            if let Some(ref next) = node.borrow().next[i] {
                // 下一个节点存在
                cmp = self
                    .cmp
                    .compare(key.data(), self.get_key_data(&node.borrow()));

                next_node = Rc::clone(&next);
            }

            // 大于下一个节点，继续沿着当前层 向右 跳
            if cmp == Ordering::Greater {
                node = Rc::clone(&next_node);
                continue;
            }

            // 走到这里，说明： node 小于等于下一个节点 或 下一个节点是空

            // 如果不保存前向节点，只是普通的搜索，找到匹配就直接返回
            if (pre_node.is_none()) && cmp == Ordering::Equal {
                return (next_node, true);
            }

            // 如果保存前向节点node
            if let Some(ref mut pre) = pre_node {
                pre.push(Rc::clone(&node));
            }

            if i == 0 {
                return (next_node, cmp == Ordering::Equal);
            }

            i -= 1;
        }
    }
    // 查询小于 key 的，离key最近的节点。这个不涉及保存pre_node 逻辑简单
    fn find_lessthan(&self, key: &internalKey) -> RcNode {
        let mut node = Rc::clone(&self.head); // 从头节点开始
        let mut next_node = Rc::clone(&node);
        let mut i = self.max_height - 1;
        loop {
            // 这里将 cmp 预先设置为 Ordering::Less 是一个非常巧妙的方式, 就可以自动包含 下个节点为空(当作是无穷大)的情况了
            let mut cmp = Ordering::Less;
            if let Some(ref next) = node.borrow().next[i] {
                // 下一个节点存在
                cmp = self
                    .cmp
                    .compare(key.data(), self.get_key_data(&node.borrow()));

                next_node = Rc::clone(&next);
            }

            // 当前链表到尾部了 或 下个节点的key 大于等于 key， 向下一层
            if cmp != Ordering::Greater {
                if i == 0 {
                    return node;
                }
                i -= 1;
            } else {
                // 当前链表 跳到 下一个节点
                node = Rc::clone(&next_node);
            }
        }
    }

    // 查询最后一个 ,逻辑也简单，每一层沿着链表走到尾部，然后跳到下一层，再沿着链表走到底
    pub fn find_last(&self) -> RcNode {
        let mut node = Rc::clone(&self.head); // 从头节点开始
        let mut next_node = Rc::clone(&node);
        let mut i = self.max_height - 1;

        loop {
            let mut end = false;
            if let Some(ref next) = node.borrow().next[i] {
                // 下一个节点存在
                next_node = Rc::clone(&next);
                end = true;
            }

            // 当前链表走到头
            if end {
                if i == 0 {
                    return node;
                }
                i -= 1;
            } else {
                node = Rc::clone(&next_node);
            }
        }
    }
}

type RcNode = Rc<RefCell<node>>;
// 每一个节点
struct node {
    offset: usize,             // 对应kv_data 中的起始位置
    key_len: usize,            // key的长度
    value_len: usize,          // value的长度
    next: Vec<Option<RcNode>>, // 当前节点有 height 层, 第i元素表示第i层的下一个节点
}
impl node {
    fn new(offset: usize, key_len: usize, value_len: usize, height: usize) -> Self {
        node {
            offset: offset,
            key_len: key_len,
            value_len: value_len,
            next: vec![None; height],
        }
    }
    // 根据 node 在 node_data中的位置，求出在kv_data 中的偏移量和长度,从而得到 key
    fn get_key_data<'a>(&self, node: &node, kv_data: &'a [u8]) -> &'a [u8] {
        &kv_data[node.offset..node.offset + node.key_len]
    }

    // 根据 node 在 node_data 中的位置，求出在kv_data 中的偏移量和长度,从而得到 value
    fn get_value_data<'a>(&self, node: &node, kv_data: &'a [u8]) -> &'a [u8] {
        &kv_data[node.offset + node.key_len..node.offset + node.key_len + node.value_len]
    }
}

pub struct DBSkip<T: Comparer> {
    db: sync::RwLock<db_skip<T>>,
}

impl<T: Comparer> DBSkip<T> {
    pub fn put(&mut self, key: &internalKey, value: Option<&[u8]>) -> GenericResult<()> {
        let mut db = self.db.write().unwrap();
        let mut pre_node = Some(vec![]);
        let (node, exist) = db.find_great_or_equal(key, &mut pre_node);
        if exist {
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
            node.borrow_mut().offset = offset;

            // value 的长度可能也变化了
            // 之前的长度
            let v_old = node.borrow().value_len;
            // 更新为新的长度
            node.borrow_mut().value_len = v_len;
            // 更新数据总大小
            db.kv_size += v_len - v_old;
            return Ok(());
        }

        let mut pre_node = pre_node.unwrap();
        let h = db.rand_heigth();
        // 处理head节点
        if h > db.max_height {
            // 补充 高出的部分
            for i in db.max_height..h{
                pre_node.push(Rc::clone(&db.head));
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

        // 创建新节点
        let node = Rc::new(RefCell::new(node::new(
            kv_offset,
            key.data().len(),
            v_len,
            h,
        )));
       
        // 执行插入
        for (i, pre) in pre_node.iter().enumerate() {
            // 新节点->next  =  pre->next
            if let Some(ref pre_next) = pre.borrow().next[i] {
                node.borrow_mut().next[i] = Some(Rc::clone(pre_next));
            }

            // pre->next = 新节点
            pre.borrow_mut().next[i] = Some(Rc::clone(&node));
        }

        db.kv_size+=key.data().len()+v_len;
        db.n+=1;

        Ok(())
    }

    pub fn delete(&mut self, key: &internalKey) -> Option<()> {
        let mut db = self.db.write().unwrap();
        let mut pre_node = Some(vec![]);
        let (node, exist) = db.find_great_or_equal(key, &mut pre_node);
        if !exist{
            return None;
        }
        let pre_node = pre_node.unwrap();
        // 执行删除
        for (i, pre) in pre_node.iter().enumerate() {
            // pre->next = node->next
            if let Some(ref node_next) = node.borrow().next[i] {
                pre.borrow_mut().next[i] = Some(Rc::clone(node_next));
            }
        }

        db.kv_size-=node.borrow().key_len+node.borrow().value_len;
        db.n -=1;
        Some(())
    }
}
