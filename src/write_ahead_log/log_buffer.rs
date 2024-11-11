pub struct Lsn {
    pub page_id: u32,
    pub slot_id: u16,
}

pub const LSN_SIZE: usize = 6;

impl Lsn {
    pub fn new(page_id: u32, slot_id: u16) -> Self {
        Lsn { page_id, slot_id }
    }

    pub fn to_bytes(&self) -> [u8; 6] {
        let mut bytes = [0; 6];
        bytes[0..4].copy_from_slice(&self.page_id.to_le_bytes());
        bytes[4..6].copy_from_slice(&self.slot_id.to_le_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8; 6]) -> Self {
        let page_id = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let slot_id = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        Lsn { page_id, slot_id }
    }
}
