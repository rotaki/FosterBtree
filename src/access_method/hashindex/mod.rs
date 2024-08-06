mod shortkeypage;

pub mod pagedhashmap;
pub mod rusthashmap;

pub mod prelude {
    pub use super::pagedhashmap::{PagedHashMap, PagedHashMapIter};
    pub use super::rusthashmap::RustHashMap;
}
