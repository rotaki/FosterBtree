use crate::page::{Page, PageId, AVAILABLE_PAGE_SIZE};
use std::mem::size_of;

const OVERFLOW_PAGE_HEADER_SIZE: usize = size_of::<OverflowHeader>();

pub trait OverflowPage {
    fn new() -> Self;
    fn ofp_init(&mut self);

    fn ofp_insert(&mut self, val: &[u8]) -> Result<(), OverflowPageError>;
    fn ofp_upsert(&mut self, val: &[u8]) -> Result<(), OverflowPageError>;
    fn ofp_get(&self) -> Result<Vec<u8>, OverflowPageError>;

    fn ofp_get_next_page_id(&self) -> PageId;
    fn ofp_set_next_page_id(&mut self, next_page_id: PageId);

    fn ofp_get_next_frame_id(&self) -> u32;
    fn ofp_set_next_frame_id(&mut self, next_frame_id: u32);

    fn encode_overflow_header(&mut self, header: &OverflowHeader);
    fn decode_overflow_header(&self) -> OverflowHeader;

    fn get_max_val_len(&self) -> usize;
}

pub struct OverflowHeader {
    next_page_id: PageId,
    next_frame_id: u32,
    val_len: u32,
}

#[derive(Debug, PartialEq)]
pub enum OverflowPageError {
    PageCapacityExceeded,
}

impl OverflowPage for Page {
    fn new() -> Self {
        let mut page = Page::new_empty();
        let header = OverflowHeader {
            next_page_id: 0,
            next_frame_id: u32::MAX,
            val_len: 0,
        };
        Self::encode_overflow_header(&mut page, &header);
        page
    }

    fn ofp_init(&mut self) {
        let header = OverflowHeader {
            next_page_id: 0,
            next_frame_id: u32::MAX,
            val_len: 0,
        };
        Self::encode_overflow_header(self, &header);
    }

    fn ofp_insert(&mut self, val: &[u8]) -> Result<(), OverflowPageError> {
        let mut header = self.decode_overflow_header();
        let available_space = AVAILABLE_PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE;

        if val.len() > available_space {
            return Err(OverflowPageError::PageCapacityExceeded);
        }

        let offset = OVERFLOW_PAGE_HEADER_SIZE;
        self[offset..offset + val.len()].copy_from_slice(val);

        header.val_len = val.len() as u32;
        self.encode_overflow_header(&header);

        Ok(())
    }

    fn ofp_upsert(&mut self, val: &[u8]) -> Result<(), OverflowPageError> {
        let mut header = self.decode_overflow_header();
        let available_space =
            AVAILABLE_PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE - header.val_len as usize;

        if val.len() > available_space {
            return Err(OverflowPageError::PageCapacityExceeded);
        }

        let offset = OVERFLOW_PAGE_HEADER_SIZE + header.val_len as usize;
        self[offset..offset + val.len()].copy_from_slice(val);

        header.val_len += val.len() as u32;
        self.encode_overflow_header(&header);

        Ok(())
    }

    fn ofp_get(&self) -> Result<Vec<u8>, OverflowPageError> {
        let header = self.decode_overflow_header();
        let offset = OVERFLOW_PAGE_HEADER_SIZE;
        let value = self[offset..offset + header.val_len as usize].to_vec();
        Ok(value)
    }

    fn ofp_get_next_page_id(&self) -> PageId {
        self.decode_overflow_header().next_page_id
    }

    fn ofp_set_next_page_id(&mut self, next_page_id: PageId) {
        let mut header = self.decode_overflow_header();
        header.next_page_id = next_page_id;
        self.encode_overflow_header(&header);
    }

    fn ofp_get_next_frame_id(&self) -> u32 {
        self.decode_overflow_header().next_frame_id
    }

    fn ofp_set_next_frame_id(&mut self, next_frame_id: u32) {
        let mut header = self.decode_overflow_header();
        header.next_frame_id = next_frame_id;
        self.encode_overflow_header(&header);
    }

    fn encode_overflow_header(&mut self, header: &OverflowHeader) {
        let offset = 0;
        self[offset..offset + size_of::<u32>()].copy_from_slice(&header.next_page_id.to_le_bytes());
        self[offset + size_of::<u32>()..offset + (2 * size_of::<u32>())]
            .copy_from_slice(&header.next_frame_id.to_le_bytes());
        self[offset + (2 * size_of::<u32>())..offset + (3 * size_of::<u32>())]
            .copy_from_slice(&header.val_len.to_le_bytes());
    }

    fn decode_overflow_header(&self) -> OverflowHeader {
        let offset = 0;
        let next_page_id = PageId::from_le_bytes(
            self[offset..offset + size_of::<u32>()]
                .try_into()
                .expect("Invalid slice length"),
        );
        let next_frame_id = u32::from_le_bytes(
            self[offset + size_of::<u32>()..offset + (2 * size_of::<u32>())]
                .try_into()
                .expect("Invalid slice length"),
        );
        let val_len = u32::from_le_bytes(
            self[offset + (2 * size_of::<u32>())..offset + (3 * size_of::<u32>())]
                .try_into()
                .expect("Invalid slice length"),
        );

        OverflowHeader {
            next_page_id,
            next_frame_id,
            val_len,
        }
    }

    fn get_max_val_len(&self) -> usize {
        AVAILABLE_PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE
    }
}
