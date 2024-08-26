use std::{sync::Arc, time::Duration};

use crate::{
    access_method::fbt::BTreeKey,
    bp::{ContainerId, DatabaseId, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    page::PageId,
    prelude::{AccessMethodError, FosterBtree, FosterBtreePage, UniqueKeyIndex},
};

pub struct FbtSecondaryLipah<T: MemPool> {
    primary: Arc<FosterBtree<T>>,
    secondary: Arc<FosterBtree<T>>,
    mempool: Arc<T>,
}

pub struct LipahKey {
    pub logical_id: Vec<u8>,
    pub physical_address: PageId, // PageID
}

impl LipahKey {
    pub fn new(logical_id: Vec<u8>, physical_address: PageId) -> Self {
        Self {
            logical_id,
            physical_address,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        // logical_id_size + logical_id + physical_address
        let mut bytes = Vec::with_capacity(4 + self.logical_id.len() + 4);
        bytes.extend_from_slice(&(self.logical_id.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&self.logical_id);
        bytes.extend_from_slice(&self.physical_address.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let logical_id_size = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let logical_id = bytes[4..4 + logical_id_size as usize].to_vec();
        let page_id = u32::from_be_bytes(
            bytes[4 + logical_id_size as usize..8 + logical_id_size as usize]
                .try_into()
                .unwrap(),
        );
        Self {
            logical_id,
            physical_address: page_id,
        }
    }
}

impl<T: MemPool> FbtSecondaryLipah<T> {
    pub fn new(
        primary_fbt: Arc<FosterBtree<T>>,
        secondary_fbt: Arc<FosterBtree<T>>,
        mempool: Arc<T>,
    ) -> Self {
        Self {
            primary: primary_fbt,
            secondary: secondary_fbt,
            mempool,
        }
    }

    fn read_page(
        &self,
        physical_address: PageFrameKey,
    ) -> Result<FrameReadGuard, AccessMethodError> {
        let base = 2;
        let mut attempt = 0;
        loop {
            match self.mempool.get_page_for_read(physical_address) {
                Ok(page) => return Ok(page),
                Err(MemPoolStatus::FrameReadLatchGrantFailed)
                | Err(MemPoolStatus::CannotEvictPage) => {
                    std::thread::sleep(Duration::from_nanos(u64::pow(base, attempt)));
                    attempt += 1;
                }
                Err(e) => {
                    panic!("Error reading page: {:?}", e);
                }
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError> {
        loop {
            let leaf_page = self.secondary.traverse_to_leaf_for_read(key);
            let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::Normal(key)) - 1;
            if slot_id == 0 {
                // Lower fence. Non-existent key
                return Err(AccessMethodError::KeyNotFound);
            } else {
                if leaf_page.get_raw_key(slot_id) == key {
                    // Logical ID is the key in the primary FBT
                    // Physical address is the page_id of the page that contains the value in the primary FBT
                    let mut val = leaf_page.get_val(slot_id).to_vec();
                    let lipah_key = LipahKey::from_bytes(&val);
                    let logical_id = &lipah_key.logical_id;
                    let page_id = lipah_key.physical_address;
                    let page_frame_key = PageFrameKey::new(self.primary.c_key, page_id);

                    {
                        // Use the cached physical address to find the key in the primary FBT
                        let page = self.read_page(page_frame_key)?;
                        if page.is_valid()
                            && page.is_leaf()
                            && page.inside_range(&BTreeKey::Normal(&logical_id))
                        {
                            let p_slot_id =
                                page.upper_bound_slot_id(&BTreeKey::Normal(&logical_id)) - 1;
                            if p_slot_id == 0 {
                                panic!("Key should exist. Lower fence of the primary FBT.")
                            } else {
                                // We can get the key if it exists
                                if page.get_raw_key(slot_id) == logical_id {
                                    return Ok(page.get_val(slot_id).to_vec());
                                } else {
                                    // Non-existent key
                                    panic!("Key should exist. Non-existent key in the primary FBT.")
                                }
                            }
                        }
                    }

                    // Need to re-traverse the primary FBT
                    let (result, (new_page_id, _)) =
                        self.primary.get_with_physical_address(&logical_id)?;
                    // Try upgrading the read-latch to write-latch. If it fails, someone is holding either
                    // the write latch or the read latch. So we should back off and retry.
                    match leaf_page.try_upgrade(true) {
                        Ok(mut write_leaf_page) => {
                            // Modify the last 4 bytes of the value to the new page_id
                            let val_len = val.len();
                            val[val_len - 4..].copy_from_slice(&new_page_id.to_be_bytes());
                            assert!(write_leaf_page.update_at(slot_id, None, &val));
                            return Ok(result);
                        }
                        Err(_) => {
                            // Need to backoff and retry
                            continue;
                        }
                    }
                } else {
                    // Non-existent key
                    return Err(AccessMethodError::KeyNotFound);
                }
            }
        }
    }
}
