use std::sync::Arc;

use crate::{
    buffer_pool::prelude::*,
    page::{Page, PageId},
    write_ahead_log::{prelude::LogRecord, LogBufferRef},
};

use super::foster_btree_page::{BTreeKey, FosterBtreePage};

pub enum TreeStatus {
    Ok,
    NotFound,
    NotInPageRange,
    Duplicate,
    NotReadyForPhysicalDelete,
    BPStatus(BPStatus),
}

impl From<BPStatus> for TreeStatus {
    fn from(status: BPStatus) -> Self {
        TreeStatus::BPStatus(status)
    }
}

pub struct FosterBtree {
    pub c_key: ContainerKey,
    pub root_key: PageKey,
    pub bp: BufferPoolRef,
    // pub wal_buffer: LogBufferRef,
}

impl FosterBtree {
    /// System transaction that allocates a new page.
    fn allocate_page(&self) -> PageKey {
        let mut foster_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
        let page_key = foster_page.key().unwrap();
        // Write log
        // {
        //     let log_record = LogRecord::SysTxnAllocPage {
        //         txn_id: 0,
        //         page_id: page_key.page_id,
        //     };
        //     let lsn = self.wal_buffer.append_log(&log_record.to_bytes());
        //     foster_page.set_lsn(lsn);
        // }
        page_key
    }

    /// Move half of the slots to the foster child
    /// If this is the right-most page, then the foster child will also be the right most page.
    /// * This means that the high fence of the foster child will be the same as the high fence of this page.
    /// If this is the left-most page, then the foster child iwll *NOT* be the left-most page.
    /// * This means that the high fence of the foster child will be the same as the low fence of the foster child.
    fn split(this: &mut Page, foster_child: &mut Page) {
        if this.active_slot_count() - 2 < 2 {
            // -2 to exclude the low and high fences.
            // Minimum 4 slots are required to split.
            panic!("Cannot split the page with less than 2 real slots");
        }

        // This page keeps [0, mid) slots and mid key with the foster_child_page_id
        // Foster child keeps [mid, active_slot_count) slots

        let mid = this.active_slot_count() / 2;
        assert!(!this.is_fence(mid));

        {
            // Set the foster_child's low and high fence and foster children flag
            let mid_key = this.get_raw_key(mid);
            foster_child.insert_low_fence(mid_key);
            if this.is_right_most() {
                // If this is the right-most page, then the foster child will also be the right most page.
                foster_child.insert_high_fence(&[]);
                foster_child.set_right_most(true);
            } else {
                let high_fence = this.get_high_fence().unwrap();
                foster_child.insert_high_fence(high_fence);
            }
            if this.has_foster_child() {
                // If this page has foster children, then the foster child will also have foster children.
                foster_child.set_foster_child(true);
            }
            if this.is_leaf() {
                // If this page is a leaf, then the foster child will also be a leaf.
                foster_child.set_leaf(true);
            }
            assert!(foster_child.active_slot_count() == 2);
        }

        {
            // Move the half of the slots to the foster child
            let recs = {
                let mut recs = Vec::new();
                for i in mid..this.high_fence_slot_id() {
                    // Does snot include the high fence
                    recs.push((this.get_raw_key(i), this.get_val(i)));
                }
                recs
            };
            let res = foster_child.insert_sorted(recs);
            assert!(res);
        }

        {
            // Remove the moved slots from this page. Do not remove the high fence. Insert the foster key with the foster_child_page_id.
            let foster_key = this.get_raw_key(mid).to_owned();
            let foster_page_id_bytes = foster_child.get_id().to_be_bytes();
            let end = this.high_fence_slot_id();
            this.remove_range(mid, end);
            this.insert(&foster_key, &foster_page_id_bytes, false);
            this.compact_space();

            #[cfg(debug_assertions)]
            {
                // Check that foster key is in the correct position.
                let foster_slot_id = this.lower_bound_slot_id(&BTreeKey::new(&foster_key));
                assert!(foster_slot_id == this.foster_child_slot_id());
            }

            // Mark that this page has foster children
            this.set_foster_child(true);
        }

        #[cfg(debug_assertions)]
        {
            this.run_consistency_checks(true);
            foster_child.run_consistency_checks(true);
        }
    }

    pub fn create_new(
        txn_id: u64,
        c_key: ContainerKey,
        bp: BufferPoolRef,
        // wal_buffer: LogBufferRef,
    ) -> Self {
        // Create a root page
        let root_key = {
            let mut root_page = bp.create_new_page_for_write(c_key).unwrap();
            root_page.init_as_root();
            let root_key = root_page.key().unwrap();
            // Write log
            // {
            //     let log_record = LogRecord::SysTxnAllocPage {
            //         txn_id,
            //         page_id: root_key.page_id,
            //     };
            //     let lsn = wal_buffer.append_log(&log_record.to_bytes());
            //     root_page.set_lsn(lsn);
            // }
            root_key
        };
        FosterBtree {
            c_key,
            root_key,
            bp: bp.clone(),
            // wal_buffer,
        }
    }

    fn traverse_to_leaf_for_read(&self, key: &[u8]) -> Result<FrameReadGuard, TreeStatus> {
        let mut current_page = self.bp.get_page_for_read(self.root_key)?;
        loop {
            let foster_page = &current_page;
            if foster_page.is_leaf() {
                break;
            }
            let page_key = {
                let slot_id = foster_page.lower_bound_slot_id(&BTreeKey::new(key));
                let page_id_bytes = foster_page.get_val(slot_id);
                let page_id = PageId::from_be_bytes(page_id_bytes.try_into().unwrap());
                PageKey::new(self.c_key, page_id)
            };

            let next_page = self.bp.get_page_for_read(page_key)?;
            // Now we have two locks. We need to release the lock of the current page.

            current_page = next_page;
        }
        Ok(current_page)
    }

    fn traverse_to_leaf_for_write(&self, key: &[u8]) -> Result<FrameWriteGuard, TreeStatus> {
        let mut current_page = self.bp.get_page_for_write(self.root_key)?;
        loop {
            let foster_page = &current_page;
            if foster_page.is_leaf() {
                break;
            }
            let page_key = {
                let slot_id = foster_page.lower_bound_slot_id(&BTreeKey::new(key));
                let page_id_bytes = foster_page.get_val(slot_id);
                let page_id = PageId::from_be_bytes(page_id_bytes.try_into().unwrap());
                PageKey::new(self.c_key, page_id)
            };

            let next_page = self.bp.get_page_for_write(page_key)?;
            // Check if there is foster child in the next page.
            if next_page.has_foster_child() {
                let new_page_key = self.allocate_page();
                let new_page = self.bp.get_page_for_write(new_page_key).unwrap();
            }

            // Now we have two locks. We need to release the lock of the current page.

            current_page = next_page;
        }
        Ok(current_page)
    }

    pub fn get_key(&self, key: &[u8]) -> Result<Vec<u8>, TreeStatus> {
        let foster_page = self.traverse_to_leaf_for_read(key)?;
        let slot_id = foster_page.lower_bound_slot_id(&BTreeKey::new(key));
        if foster_page.get_btree_key(slot_id) == BTreeKey::new(key) {
            // Exact match
            if foster_page.is_ghost_slot(slot_id) {
                Err(TreeStatus::NotFound)
            } else {
                Ok(foster_page.get_val(slot_id).to_vec())
            }
        } else {
            // Non-existent key
            Err(TreeStatus::NotFound)
        }
    }

    /*
    pub fn insert_key(&self, key: &[u8], value: &[u8]) -> Result<(), TreeStatus> {
        let mut foster_page = self.traverse_to_leaf_for_write(key)?; // Hold X latch on the page
        let rec = foster_page.lower_bound_rec(key).ok_or(TreeStatus::NotInPageRange)?;
        if rec.key == key {
            // Exact match
            if !rec.is_ghost {
                return Err(TreeStatus::Duplicate)
            } else {
                // Replace ghost record
                foster_page.replace_ghost(key, value);
            }
        } else {
            foster_page = {
                // System transaction
                // * Tries to insert the key-value pair into the page
                // * If the page is full, then split the page and insert the key-value pair
                // * If the page is split, then the parent page is also split
                let inserted = foster_page.insert(key, value, true);
                if !inserted {
                    // Split the page
                    let (new_page, new_key) = foster_page.split();
                    // Insert the new key into the parent page
                    foster_page.insert_into_parent(new_key, new_page)
                }
                foster_page
            };
            {
                foster_page.replace_ghost(key, value);
            }
        }
        Ok(())
    }

    // Logical deletion of a key
    pub fn delete_key(&self, key: &[u8]) -> Result<(), TreeStatus> {
        let mut leaf = self.traverse_to_leaf_for_write(key)?; // Hold X latch on the page
        let mut foster_page = FosterBtreePage::new(&mut *leaf);
        let rec = foster_page.lower_bound_rec(key).ok_or(TreeStatus::NotInPageRange)?;
        if rec.key == key {
            // Exact match
            if rec.is_ghost {
                Err(TreeStatus::NotFound)
            } else {
                // Logical deletion
                foster_page.mark_ghost(key);
                Ok(())
            }
        } else {
            // Non-existent key
            Err(TreeStatus::NotFound)
        }
    }

    pub fn update_key(&self, key: &[u8], value: &[u8]) -> Result<(), TreeStatus> {
        let mut leaf = self.traverse_to_leaf_for_write(key)?; // Hold X latch on the page
        let mut foster_page = FosterBtreePage::new(&mut *leaf);
        let rec = foster_page.lower_bound_rec(key).ok_or(TreeStatus::NotInPageRange)?;
        if rec.key == key {
            // Exact match
            if rec.is_ghost {
                Err(TreeStatus::NotFound)
            } else {
                // Update the value
                foster_page.update(key, value);
                Ok(())
            }
        } else {
            // Non-existent key
            Err(TreeStatus::NotFound)
        }
    }

    pub fn physical_delete_key(&self, key: &[u8]) -> Result<(), TreeStatus> {
        let mut leaf = self.traverse_to_leaf_for_write(key)?; // Hold X latch on the page
        let mut foster_page = FosterBtreePage::new(&mut *leaf);
        let rec = foster_page.lower_bound_rec(key).ok_or(TreeStatus::NotInPageRange)?;
        if rec.key == key {
            // Exact match
            if rec.is_ghost {
                // Check lock conflicts and if there is no physical conflict, then delete the key
                {
                    // System transaction
                    foster_page.remove(key);
                }
                Ok(())
            } else {
                Err(TreeStatus::NotReadyForPhysicalDelete)
            }
        } else {
            // Non-existent key
            Err(TreeStatus::NotFound)
        }
    }
    */
}

mod tests {
    use core::num;
    use std::collections::btree_map::Keys;

    use tempfile::TempDir;

    use crate::{buffer_pool::CacheEvictionPolicy, foster_btree::foster_btree_page::BTreeKey};

    use super::{BufferPool, BufferPoolRef, ContainerKey, FosterBtree, FosterBtreePage};

    fn get_buffer_pool(db_id: u16) -> (TempDir, BufferPoolRef) {
        let temp_dir = TempDir::new().unwrap();
        // create a directory for the database
        std::fs::create_dir(temp_dir.path().join(db_id.to_string())).unwrap();
        let num_frames = 10;
        let ep = CacheEvictionPolicy::new(num_frames);
        let bp = BufferPoolRef::new(BufferPool::new(temp_dir.path(), num_frames, ep).unwrap());
        (temp_dir, bp)
    }

    fn get_btree(db_id: u16) -> (TempDir, FosterBtree) {
        let (temp_dir, bp) = get_buffer_pool(db_id);
        let c_key = ContainerKey::new(db_id, 0);
        let txn_id = 0;
        let btree = FosterBtree::create_new(txn_id, c_key, bp.clone());
        (temp_dir, btree)
    }

    #[test]
    fn test_page_split() {
        let db_id = 0;
        let c_id = 0;
        let c_key = ContainerKey::new(db_id, c_id);

        let (temp_dir, bp) = get_buffer_pool(db_id);
        let p0_key = bp.create_new_page_for_write(c_key).unwrap().key().unwrap();
        let mut p0 = bp.get_page_for_write(p0_key).unwrap();
        p0.init_as_root();
        let p1_key = bp.create_new_page_for_write(c_key).unwrap().key().unwrap();
        let mut p1 = bp.get_page_for_write(p1_key).unwrap();
        p1.init();

        // Insert 10 keys into p0
        let num_keys = 10;
        let kvs = (0..num_keys as usize)
            .map(|i| (i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()))
            .collect::<Vec<_>>();
        p0.insert_sorted(
            kvs.iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice()))
                .collect(),
        );
        assert_eq!(p0.active_slot_count(), num_keys + 2);

        // Split p0 into p0 and p1
        FosterBtree::split(&mut p0, &mut p1);

        // Check the contents of p0 and p1
        p0.run_consistency_checks(true);
        p1.run_consistency_checks(true);

        // Check the contents of p0
        // p0 has 0, 1, 2, 3, 4, and foster key 5
        assert_eq!(p0.active_slot_count(), 8); // 5 real slots + 1 foster key + 2 fences
        assert!(p0.has_foster_child());
        assert_eq!(p0.get_foster_page_id(), p1.get_id());
        for i in 0..5 as usize {
            let key = p0.get_raw_key((i + 1) as u16);
            assert_eq!(key, i.to_be_bytes());
        }

        // Check the contents of p1
        // p1 has 5, 6, 7, 8, 9
        assert_eq!(p1.active_slot_count(), 7); // 5 real slots + 2 fences
        assert!(!p1.has_foster_child());
        for i in 0..5 as usize {
            let key = p1.get_raw_key((i + 1) as u16);
            assert_eq!(key, (i + 5).to_be_bytes());
        }

        drop(temp_dir);
    }
}