use std::ops::{Deref, DerefMut};

use crate::write_ahead_log::prelude::{Lsn, LSN_SIZE};

// A slot offset is u32, so the maximum page size is 2^32 bytes
#[cfg(feature = "4k_page")]
pub const PAGE_SIZE: usize = 4096;
#[cfg(feature = "8k_page")]
pub const PAGE_SIZE: usize = 8192;
#[cfg(feature = "16k_page")]
pub const PAGE_SIZE: usize = 16384;
#[cfg(feature = "32k_page")]
pub const PAGE_SIZE: usize = 32768;
#[cfg(feature = "64k_page")]
pub const PAGE_SIZE: usize = 65536;
#[cfg(feature = "128k_page")]
pub const PAGE_SIZE: usize = 131072;
#[cfg(feature = "256k_page")]
pub const PAGE_SIZE: usize = 262144;
#[cfg(feature = "512k_page")]
pub const PAGE_SIZE: usize = 524288;
#[cfg(feature = "1m_page")]
pub const PAGE_SIZE: usize = 1048576;
// If nothing is specified, use 16K page
#[cfg(not(any(
    feature = "4k_page",
    feature = "8k_page",
    feature = "16k_page",
    feature = "32k_page",
    feature = "64k_page",
    feature = "128k_page",
    feature = "256k_page",
    feature = "512k_page",
    feature = "1m_page"
)))]
pub const PAGE_SIZE: usize = 16384;

pub type PageId = u32;
const BASE_PAGE_HEADER_SIZE: usize = 4 + LSN_SIZE;
pub const AVAILABLE_PAGE_SIZE: usize = PAGE_SIZE - BASE_PAGE_HEADER_SIZE;

#[cfg(feature = "heap_allocated_page")]
#[repr(C, align(4096))]
pub struct Page(Vec<u8>); // A page with large size must be heap allocated. Otherwise, it will cause stack overflow during the test.
#[cfg(not(feature = "heap_allocated_page"))]
#[repr(C, align(4096))] // Align to 4K. If we don't align to 4K, it might break O_DIRECT writes.
pub struct Page([u8; PAGE_SIZE]);

impl Page {
    pub fn new(page_id: PageId) -> Self {
        #[cfg(feature = "heap_allocated_page")]
        let mut page = Page(vec![0; PAGE_SIZE]);
        #[cfg(not(feature = "heap_allocated_page"))]
        let mut page = Page([0; PAGE_SIZE]);
        page.set_id(page_id);
        page.set_lsn(Lsn::new(0, 0));
        page
    }

    pub fn new_empty() -> Self {
        #[cfg(feature = "heap_allocated_page")]
        return Page(vec![0; PAGE_SIZE]);
        #[cfg(not(feature = "heap_allocated_page"))]
        return Page([0; PAGE_SIZE]);
    }

    pub fn copy(&mut self, other: &Page) {
        self.0.copy_from_slice(&other.0);
    }

    pub fn copy_data_only(&mut self, other: &Page) {
        self.0[BASE_PAGE_HEADER_SIZE..].copy_from_slice(&other.0[BASE_PAGE_HEADER_SIZE..]);
    }

    fn base_header(&self) -> BasePageHeader {
        BasePageHeader::from_bytes(&self.0[0..BASE_PAGE_HEADER_SIZE].try_into().unwrap())
    }

    pub fn get_id(&self) -> PageId {
        self.base_header().id
    }

    pub fn set_id(&mut self, id: PageId) {
        let mut header = self.base_header();
        header.id = id;
        self.0[0..BASE_PAGE_HEADER_SIZE].copy_from_slice(&header.to_bytes());
    }

    pub fn get_lsn(&self) -> Lsn {
        self.base_header().lsn
    }

    pub fn set_lsn(&mut self, lsn: Lsn) {
        let mut header = self.base_header();
        header.lsn = lsn;
        self.0[0..BASE_PAGE_HEADER_SIZE].copy_from_slice(&header.to_bytes());
    }

    pub fn get_raw_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn get_raw_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

struct BasePageHeader {
    id: u32,
    lsn: Lsn,
}

impl BasePageHeader {
    fn from_bytes(bytes: &[u8; BASE_PAGE_HEADER_SIZE]) -> Self {
        let id = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let lsn = Lsn::from_bytes(&bytes[4..4 + LSN_SIZE].try_into().unwrap());
        BasePageHeader { id, lsn }
    }

    fn to_bytes(&self) -> [u8; BASE_PAGE_HEADER_SIZE] {
        let id_bytes = self.id.to_be_bytes();
        let lsn_bytes = self.lsn.to_bytes();
        let mut bytes = [0; BASE_PAGE_HEADER_SIZE];
        bytes[0..4].copy_from_slice(&id_bytes);
        bytes[4..4 + LSN_SIZE].copy_from_slice(&lsn_bytes);
        bytes
    }
}

impl Deref for Page {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0[BASE_PAGE_HEADER_SIZE..]
    }
}

impl DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0[BASE_PAGE_HEADER_SIZE..]
    }
}
