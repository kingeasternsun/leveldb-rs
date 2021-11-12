#![feature(partition_point)]
pub mod errors;
pub mod key;
pub mod memdb;
pub use errors::GenericResult;

pub mod compare;
pub mod table;
pub use compare::BytesComparer;
pub use compare::Comparer;

pub mod icompare;
pub mod storage;
