mod foster_btree;
mod foster_btree_page;
mod foster_btree_visualizer_wasm;

pub use foster_btree::{
    FosterBtree, FosterBtreeAppendOnly, FosterBtreeAppendOnlyCursor,
    FosterBtreeAppendOnlyRangeScanner, FosterBtreeAppendOnlyRangeScannerWithPageId,
    FosterBtreeCursor, FosterBtreeRangeScanner, FosterBtreeRangeScannerWithPageId,
    READ_HINT_ACCESS, READ_HINT_ASSISTED_ACCESS, READ_NORMAL_ACCESS, WRITE_HINT_ACCESS,
    WRITE_HINT_ASSISTED_ACCESS, WRITE_NORMAL_ACCESS,
};
pub(crate) use foster_btree_page::BTreeKey;
pub use foster_btree_page::FosterBtreePage;

#[cfg(target_arch = "wasm32")]
pub use foster_btree_visualizer_wasm::inner::FosterBtreeVisualizer;

pub mod prelude {
    pub use super::{FosterBtree, FosterBtreePage};
}
