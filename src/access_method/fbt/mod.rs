mod foster_btree;
mod foster_btree_page;
mod foster_btree_visualizer_wasm;

pub use foster_btree::{
    FosterBtree, FosterBtreeAppendOnly, FosterBtreeAppendOnlyCursor,
    FosterBtreeAppendOnlyRangeScanner, FosterBtreeCursor, FosterBtreeRangeScanner,
};
pub use foster_btree_page::BTreeKey;
pub use foster_btree_page::FosterBtreePage;

#[cfg(target_arch = "wasm32")]
pub use foster_btree_visualizer_wasm::inner::FosterBtreeVisualizer;

pub mod prelude {
    pub use super::{FosterBtree, FosterBtreePage};
}
