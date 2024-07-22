use std::ops::{Deref, DerefMut};

use crate::write_ahead_log::prelude::{Lsn, LSN_SIZE};

#[cfg(feature = "4k_page")]
pub const PAGE_SIZE: usize = 4096;
#[cfg(feature = "8k_page")]
pub const PAGE_SIZE: usize = 8192;
#[cfg(feature = "16k_page")]
pub const PAGE_SIZE: usize = 16384;
#[cfg(feature = "32k_page")]
pub const PAGE_SIZE: usize = 32768;

pub type PageId = u32;
const BASE_PAGE_HEADER_SIZE: usize = 4 + LSN_SIZE;
pub const AVAILABLE_PAGE_SIZE: usize = PAGE_SIZE - BASE_PAGE_HEADER_SIZE;

pub struct Page([u8; PAGE_SIZE]);

impl Page {
    pub fn new(page_id: PageId) -> Self {
        let mut page = Page([0; PAGE_SIZE]);
        page.set_id(page_id);
        page.set_lsn(Lsn::new(0, 0));
        page
    }

    pub fn new_empty() -> Self {
        Page([0; PAGE_SIZE])
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
