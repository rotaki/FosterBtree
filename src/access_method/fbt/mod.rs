mod foster_btree;
// mod foster_btree_page;
mod foster_btree_page_prefix_key;
mod foster_btree_visualizer_wasm;

pub use foster_btree::{
    FosterBtree, FosterBtreeAppendOnly, FosterBtreeAppendOnlyCursor,
    FosterBtreeAppendOnlyRangeScanner, FosterBtreeAppendOnlyRangeScannerWithPageId,
    FosterBtreeCursor, FosterBtreeRangeScanner, FosterBtreeRangeScannerWithPageId,
};
// pub(crate) use foster_btree_page::BTreeKey;
// pub use foster_btree_page::FosterBtreePage;
pub(crate) use foster_btree_page_prefix_key::BTreeKey;
pub use foster_btree_page_prefix_key::FosterBtreePage;

#[cfg(target_arch = "wasm32")]
pub use foster_btree_visualizer_wasm::inner::FosterBtreeVisualizer;

pub mod prelude {
    pub use super::{FosterBtree, FosterBtreePage};
}
