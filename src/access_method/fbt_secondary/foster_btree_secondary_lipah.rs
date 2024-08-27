use std::{sync::Arc, time::Duration};

use crate::{
    access_method::fbt::BTreeKey,
    bp::{ContainerId, DatabaseId, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    page::PageId,
    prelude::{AccessMethodError, FosterBtree, FosterBtreePage, UniqueKeyIndex},
};

pub struct FbtSecondaryLipah<T: MemPool> {
    pub primary: Arc<FosterBtree<T>>,
    pub secondary: Arc<FosterBtree<T>>,
    pub mempool: Arc<T>,
}

pub struct LipahKey {
    pub logical_id: Vec<u8>,
    pub physical_address: (PageId, u32), // PageID
}

impl LipahKey {
    pub fn new(logical_id: Vec<u8>, physical_address: (PageId, u32)) -> Self {
        Self {
            logical_id,
            physical_address,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        // logical_id_size + logical_id + physical_address
        let mut bytes = Vec::with_capacity(4 + self.logical_id.len() + 8);
        bytes.extend_from_slice(&(self.logical_id.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&self.logical_id);
        bytes.extend_from_slice(&self.physical_address.0.to_be_bytes());
        bytes.extend_from_slice(&self.physical_address.1.to_be_bytes());
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
        let frame_id = u32::from_be_bytes(
            bytes[8 + logical_id_size as usize..12 + logical_id_size as usize]
                .try_into()
                .unwrap(),
        );
        Self {
            logical_id,
            physical_address: (page_id, frame_id),
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
                    let (page_id, frame_id) = lipah_key.physical_address;
                    let page_frame_key =
                        PageFrameKey::new_with_frame_id(self.primary.c_key, page_id, frame_id);

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
                                if page.get_raw_key(p_slot_id) == logical_id {
                                    return Ok(page.get_val(p_slot_id).to_vec());
                                } else {
                                    // Non-existent key
                                    panic!("Key should exist. Non-existent key in the primary FBT.")
                                }
                            }
                        }
                    }

                    // Need to re-traverse the primary FBT
                    let (result, (new_page_id, new_frame_id)) =
                        self.primary.get_with_physical_address(&logical_id)?;
                    // Try upgrading the read-latch to write-latch. If it fails, someone is holding either
                    // the write latch or the read latch. So we should back off and retry.
                    match leaf_page.try_upgrade(true) {
                        Ok(mut write_leaf_page) => {
                            // Modify the last 4 bytes of the value to the new page_id
                            let val_len = val.len();
                            val[val_len - 8..val_len - 4]
                                .copy_from_slice(&new_page_id.to_be_bytes());
                            val[val_len - 4..].copy_from_slice(&new_frame_id.to_be_bytes());
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

#[cfg(test)]
mod tests {
    // Define a primary key index and a secondary key index
    // In the test, secondary key index has the same key as the primary key index
    // The value of the secondary key index is a LipahKey
    // The logical_id is the key in the primary key index
    // The physical_address is the page_id of the page that contains the value in the primary key index
    use super::*;
    use crate::{
        access_method::fbt::FosterBtreeRangeScannerWithPageId,
        bp::{get_test_bp, BufferPool, ContainerKey},
        prelude::{FosterBtree, PAGE_SIZE},
        random::gen_random_byte_vec,
    };
    use rand::prelude::Distribution;
    use rand::Rng;
    use std::sync::{atomic::AtomicBool, Arc};

    fn get_key_bytes(key: usize) -> Vec<u8> {
        key.to_be_bytes().to_vec()
    }

    fn from_key_bytes(bytes: &[u8]) -> usize {
        usize::from_be_bytes(bytes.try_into().unwrap())
    }

    #[test]
    fn test_fbt_secondary_lipah() {
        let num_keys = 1000;
        let val = vec![1u8; 1000];

        let bp = get_test_bp(100);
        let primary = Arc::new(FosterBtree::new(ContainerKey::new(0, 0), bp.clone()));

        for i in 0..num_keys {
            let key = get_key_bytes(i);
            primary.insert(&key, &val).unwrap();
        }

        let iter = FosterBtreeRangeScannerWithPageId::new(&primary, &[], &[]);
        let secondary = Arc::new(FosterBtree::bulk_insert_create(
            ContainerKey::new(0, 1),
            bp.clone(),
            iter.map(|(key, _, phys_addr)| {
                (key.to_vec(), LipahKey::new(key, phys_addr).to_bytes())
            }),
        ));

        let lipah_index = FbtSecondaryLipah::new(primary.clone(), secondary.clone(), bp.clone());

        for i in 0..num_keys {
            let key = get_key_bytes(i);
            let val = lipah_index.get(&key).unwrap();
            assert_eq!(val, vec![1u8; 1000]);
        }
    }
}
