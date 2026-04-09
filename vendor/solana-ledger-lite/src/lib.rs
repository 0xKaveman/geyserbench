macro_rules! datapoint_info {
    ($($tt:tt)*) => {};
}

pub mod blockstore;
pub mod blockstore_meta;
pub mod shred;

mod shredder;
