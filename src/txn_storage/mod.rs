mod inmem;
mod locktable;
mod ondisk;
mod ondisk_2pl;
mod txn_storage_trait;

pub use inmem::{InMemDummyTxnHandle, InMemIterator, InMemStorage};
pub use ondisk::{OnDiskDummyTxnHandle, OnDiskIterator, OnDiskStorage};
pub use ondisk_2pl::NoWaitTxnStorage;
pub use txn_storage_trait::{
    ContainerDS, ContainerOptions, ContainerType, DBOptions, ScanOptions, TxnOptions,
    TxnStorageStatus, TxnStorageTrait,
};

pub mod prelude {
    pub use super::{
        ContainerDS, ContainerOptions, ContainerType, DBOptions, InMemDummyTxnHandle,
        InMemIterator, InMemStorage, OnDiskDummyTxnHandle, OnDiskIterator, OnDiskStorage,
        ScanOptions, TxnOptions, TxnStorageStatus, TxnStorageTrait,
    };
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use crate::log;
    use ondisk::OnDiskStorage;
    use rstest::rstest;

    #[cfg(test)]
    use super::*;
    use crate::{
        bp::{
            get_test_bp,
            prelude::{ContainerId, DatabaseId},
            BufferPool, MemPool,
        },
        container::ContainerManager,
        random::RandomKVs,
    };
    use std::{collections::HashSet, sync::Arc, thread};

    fn get_in_mem_storage() -> Arc<impl TxnStorageTrait> {
        Arc::new(InMemStorage::new())
    }

    fn get_on_disk_storage() -> Arc<impl TxnStorageTrait> {
        let bp = get_test_bp(1024);
        Arc::new(OnDiskStorage::new(&bp))
    }

    #[rstest]
    #[case::in_mem(get_in_mem_storage())]
    #[case::on_disk(get_on_disk_storage())]
    fn test_open_and_delete_db(#[case] storage: Arc<impl TxnStorageTrait>) {
        let db_options = DBOptions::new("test_db");
        let db_id = storage.open_db(db_options).unwrap();
        assert!(storage.delete_db(db_id).is_ok());
    }

    fn setup_table<T: TxnStorageTrait>(
        storage: impl AsRef<T>,
        c_type: ContainerDS,
    ) -> (DatabaseId, ContainerId) {
        let storage = storage.as_ref();
        let db_options = DBOptions::new("test_db");
        let db_id = storage.open_db(db_options).unwrap();
        let container_options = ContainerOptions::primary("test_container", c_type);
        let c_id = storage.create_container(db_id, container_options).unwrap();
        (db_id, c_id)
    }

    /*
    #[test]
    fn test_create_and_delete_container() {
        let storage = get_on_disk_storage();
        let (db_id, c_id) = setup_table(&storage, ContainerType::BTree);
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        assert!(storage.delete_container(&txn, &db_id, &c_id).is_ok());
        storage.commit_txn(&txn, false).unwrap();
    }
    */

    #[rstest]
    #[case::in_mem(get_in_mem_storage(), ContainerDS::BTree)]
    #[case::in_mem(get_in_mem_storage(), ContainerDS::Hash)]
    #[case::on_disk(get_on_disk_storage(), ContainerDS::BTree)]
    fn test_insert_and_get_value(
        #[case] storage: Arc<impl TxnStorageTrait>,
        #[case] c_type: ContainerDS,
    ) {
        let (db_id, c_id) = setup_table(&storage, c_type);
        let key = vec![0];
        let value = vec![1, 2, 3, 4];
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn, c_id, key.clone(), value.clone())
            .unwrap();
        let retrieved_value = storage.get_value(&txn, c_id, &key).unwrap();
        assert_eq!(value, retrieved_value);
        storage.commit_txn(&txn, false).unwrap();
    }

    #[rstest]
    #[case::in_mem(get_in_mem_storage(), ContainerDS::BTree)]
    #[case::in_mem(get_in_mem_storage(), ContainerDS::Hash)]
    #[case::on_disk(get_on_disk_storage(), ContainerDS::BTree)]
    fn test_update_and_remove_value(
        #[case] storage: Arc<impl TxnStorageTrait>,
        #[case] c_type: ContainerDS,
    ) {
        let (db_id, c_id) = setup_table(&storage, c_type);
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let key = vec![0];
        let value = vec![1, 2, 3, 4];
        storage
            .insert_value(&txn, c_id, key.clone(), value.clone())
            .unwrap();
        let new_value = vec![4, 3, 2, 1];
        storage
            .update_value(&txn, c_id, &key, new_value.clone())
            .unwrap();
        let updated_value = storage.get_value(&txn, c_id, &key).unwrap();
        assert_eq!(new_value, updated_value);

        assert!(storage.delete_value(&txn, c_id, &key).is_ok());
        assert!(matches!(
            storage.get_value(&txn, c_id, &key),
            Err(TxnStorageStatus::KeyNotFound)
        ));
        storage.commit_txn(&txn, false).unwrap();
    }

    #[rstest]
    #[case::in_mem(get_in_mem_storage(), ContainerDS::BTree)]
    #[case::in_mem(get_in_mem_storage(), ContainerDS::Hash)]
    #[case::in_mem(get_in_mem_storage(), ContainerDS::AppendOnly)]
    #[case::on_disk(get_on_disk_storage(), ContainerDS::BTree)]
    #[case::on_disk(get_on_disk_storage(), ContainerDS::AppendOnly)]
    fn test_insert_and_scan_range(
        #[case] storage: Arc<impl TxnStorageTrait>,
        #[case] c_type: ContainerDS,
    ) {
        let (db_id, c_id) = setup_table(&storage, c_type);

        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        let mut kvs = HashSet::new();

        // Insert some values
        for i in 0..4 {
            let key = vec![i];
            let value = vec![i; 4];
            kvs.insert((key.clone(), value.clone()));
            storage.insert_value(&txn, c_id, key, value).unwrap();
        }

        let iter_handle = storage.scan_range(&txn, c_id, ScanOptions::new()).unwrap();

        while let Ok(Some((key, val))) = storage.iter_next(&txn, &iter_handle) {
            let key = key.to_vec();
            let val = val.to_vec();
            assert!(kvs.remove(&(key.clone(), val.clone())));
        }
        assert!(kvs.is_empty());

        storage.commit_txn(&txn, false).unwrap();
    }

    #[rstest]
    #[case::in_mem(get_in_mem_storage(), ContainerDS::BTree)]
    #[case::in_mem(get_in_mem_storage(), ContainerDS::Hash)]
    #[case::in_mem(get_in_mem_storage(), ContainerDS::AppendOnly)]
    #[case::on_disk(get_on_disk_storage(), ContainerDS::BTree)]
    #[case::on_disk(get_on_disk_storage(), ContainerDS::AppendOnly)]
    fn test_concurrent_insert(
        #[case] storage: Arc<impl TxnStorageTrait>,
        #[case] c_type: ContainerDS,
    ) {
        let (db_id, c_id) = setup_table(&storage, c_type);
        let num_threads = 4;
        let num_keys_per_thread = 10000;

        let kvs = RandomKVs::new(
            true,
            false,
            num_threads,
            num_threads * num_keys_per_thread,
            50,
            50,
            100,
        );
        let mut verify_kvs = HashSet::new();
        for kv in kvs.iter() {
            for (key, value) in kv.iter() {
                verify_kvs.insert((key.clone(), value.clone()));
            }
        }

        // Use scoped threads to insert values
        thread::scope(|scope| {
            for kv in kvs.iter() {
                let storage = storage.clone();
                scope.spawn(move || {
                    for (key, value) in kv.iter() {
                        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
                        storage
                            .insert_value(&txn, c_id, key.clone(), value.clone())
                            .unwrap();
                        storage.commit_txn(&txn, false).unwrap();
                    }
                });
            }
        });

        // Check if all values are inserted
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let iter_handle = storage.scan_range(&txn, c_id, ScanOptions::new()).unwrap();
        while let Ok(Some((key, val))) = storage.iter_next(&txn, &iter_handle) {
            let key = key.to_vec();
            let val = val.to_vec();
            assert!(verify_kvs.remove(&(key.clone(), val.clone())));
        }
        assert!(verify_kvs.is_empty());
        storage.commit_txn(&txn, false).unwrap();
    }

    #[test]
    fn test_ondisk_durability() {
        let tempdir = tempfile::tempdir().unwrap();

        let (db_id, c_ids) = {
            let cm = Arc::new(ContainerManager::new(tempdir.path(), true, false).unwrap());
            let bp1 = Arc::new(BufferPool::new(10, cm).unwrap());
            let storage1 = OnDiskStorage::new(&bp1);

            let db_options = DBOptions::new("test_db");
            let db_id = storage1.open_db(db_options).unwrap();
            // Create three containers and insert 10 values to each of them
            let c_id1 = storage1
                .create_container(
                    db_id,
                    ContainerOptions::primary("test_container1", ContainerDS::BTree),
                )
                .unwrap();
            let c_id2 = storage1
                .create_container(
                    db_id,
                    ContainerOptions::primary("test_container2", ContainerDS::AppendOnly),
                )
                .unwrap();
            let c_id3 = storage1
                .create_container(
                    db_id,
                    ContainerOptions::primary("test_container3", ContainerDS::BTree),
                )
                .unwrap();

            for c_id in &[c_id1, c_id2, c_id3] {
                let txn = storage1.begin_txn(db_id, TxnOptions::default()).unwrap();
                for i in 0..10 {
                    let key = vec![i];
                    let value = vec![*c_id as u8; 4];
                    storage1.insert_value(&txn, *c_id, key, value).unwrap();
                }
                storage1.commit_txn(&txn, false).unwrap();
            }
            bp1.flush_all_and_reset().unwrap();

            (db_id, (c_id1, c_id2, c_id3))
        };

        let cm = Arc::new(ContainerManager::new(tempdir.path(), true, false).unwrap());
        let bp2 = Arc::new(BufferPool::new(10, cm).unwrap());
        let storage2 = OnDiskStorage::load(&bp2);

        // Check if the values are still present after restarting the storage
        for c_id in &[c_ids.0, c_ids.1, c_ids.2] {
            let txn = storage2.begin_txn(db_id, TxnOptions::default()).unwrap();
            let iter_handle = storage2
                .scan_range(&txn, *c_id, ScanOptions::new())
                .unwrap();
            let mut count = 0;
            while let Ok(Some((key, val))) = storage2.iter_next(&txn, &iter_handle) {
                let key = u8::from_be_bytes(key.as_slice().try_into().unwrap());
                assert_eq!(key, count);
                assert_eq!(val, vec![*c_id as u8; 4]);
                count += 1;
            }
            assert_eq!(count, 10);
        }
    }

    /*
    #[test]
    fn test_concurrent_insert_and_container_ops() {
        // Create two containers.
        // Keep inserting into the second container. Remove the first container. Create a new container at the same time.
        let storage = get_on_disk_storage();
        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let c_id1 = storage
            .create_container(
                &storage.begin_txn(&db_id, TxnOptions::default()).unwrap(),
                &db_id,
                ContainerOptions::new("test_container1", ContainerType::BTree),
            )
            .unwrap();
        let c_id2 = storage
            .create_container(
                &storage.begin_txn(&db_id, TxnOptions::default()).unwrap(),
                &db_id,
                ContainerOptions::new("test_container2", ContainerType::BTree),
            )
            .unwrap();

        let num_threads = 4; // Threads to insert into the second container
        let num_keys_per_thread = 10000;
        let mut threads = Vec::with_capacity(num_threads);
        for i in 0..num_threads {
            let storage = storage.clone();
            threads.push(thread::spawn(move || {
                for k in 0..num_keys_per_thread {
                    let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
                    let key: usize = i * num_keys_per_thread + k;
                    let key = key.to_be_bytes().to_vec();
                    let value = key.clone();
                    storage
                        .insert_value(&txn, &c_id2, key.clone(), value.clone())
                        .unwrap();
                    storage.commit_txn(&txn, false).unwrap();
                }
            }));
        }
        // Create a new container and delete the first container
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        let _c_id3 = storage
            .create_container(
                &txn,
                &db_id,
                ContainerOptions::new("test_container3", ContainerType::BTree),
            )
            .unwrap();
        storage.commit_txn(&txn, false).unwrap();
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        storage.delete_container(&txn, &db_id, &c_id1).unwrap();
        storage.commit_txn(&txn, false).unwrap();
        for t in threads {
            t.join().unwrap();
        }

        // Check if all values are inserted
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        let iter_handle = storage
            .scan_range(&txn, &c_id2, ScanOptions::new())
            .unwrap();
        let mut count = 0;
        while let Ok(Some((key, val))) = storage.iter_next(&iter_handle) {
            let key = usize::from_be_bytes(key.as_slice().try_into().unwrap());
            assert_eq!(key, count);
            assert_eq!(val, key.to_be_bytes().to_vec());
            count += 1;
        }
        assert_eq!(count, num_threads * num_keys_per_thread);
        // println!("list_containers: {:?}", storage.list_containers(&txn, &db_id).unwrap());
    }
    */
}
