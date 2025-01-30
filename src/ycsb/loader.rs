use core::panic;
use std::{collections::HashMap, ops::Index};

use crate::{
    bp::ContainerId,
    prelude::{
        ContainerDS, ContainerOptions, DBOptions, ScanOptions, TxnOptions, TxnStorageTrait, DB_ID,
    },
    random::gen_random_byte_vec,
};

use super::txn_utils::YCSBConfig;

struct KeyValueGenerator {
    key_size: usize,
    value_size: usize,
    current_key: usize, // Inclusive
    end_key: usize,     // Exclusive
}

impl KeyValueGenerator {
    pub fn new(partition: usize, num_keys: usize, key_size: usize, value_size: usize) -> Vec<Self> {
        // Divide the keys equally among the partitions and
        // assign the remaining keys to the last partition
        let num_keys_per_partition = num_keys / partition;
        let mut generators = Vec::new();
        let mut count = 0;
        for i in 0..partition {
            let start_key = count;
            let end_key = if i == partition - 1 {
                num_keys
            } else {
                count + num_keys_per_partition
            };
            count = end_key;

            generators.push(Self {
                key_size,
                value_size,
                current_key: start_key,
                end_key,
            });
        }
        generators
    }
}

impl Iterator for KeyValueGenerator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_key >= self.end_key {
            return None;
        }
        let key = get_key_bytes(self.current_key, self.key_size);
        let value = gen_random_byte_vec(self.value_size, self.value_size);
        self.current_key += 1;

        Some((key, value))
    }
}

fn get_key_bytes(key: usize, key_size: usize) -> Vec<u8> {
    if key_size < std::mem::size_of::<usize>() {
        panic!("Key size is less than the size of usize");
    }
    let mut key_vec = vec![0u8; key_size];
    let bytes = key.to_be_bytes().to_vec();
    key_vec[key_size - bytes.len()..].copy_from_slice(&bytes);
    key_vec
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum YCSBTable {
    Primary,
    Secondary,
}

#[derive(Default)]
pub struct YCSBTableInfo {
    map: HashMap<YCSBTable, ContainerId>,
}

impl YCSBTableInfo {
    pub fn new() -> Self {
        YCSBTableInfo {
            map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, table: YCSBTable, container_id: ContainerId) {
        self.map.insert(table, container_id);
    }
}

impl Index<YCSBTable> for YCSBTableInfo {
    type Output = ContainerId;

    fn index(&self, table: YCSBTable) -> &Self::Output {
        self.map.get(&table).unwrap()
    }
}

fn load_primary_and_secondary_tables(
    txn_storage: &impl TxnStorageTrait,
    table_info: &YCSBTableInfo,
    config: &YCSBConfig,
) {
    // To be safe, insert all the key values using a single thread
    let num_insertion_threads = 1;
    let mut gen = KeyValueGenerator::new(
        num_insertion_threads,
        config.num_keys,
        config.key_size,
        config.value_size,
    );
    let gen = gen.pop().unwrap();
    for (key, value) in gen {
        txn_storage
            .raw_insert_value(DB_ID, table_info[YCSBTable::Primary], key.clone(), value)
            .unwrap();
        let sec_value = key.clone();
        txn_storage
            .raw_insert_value(DB_ID, table_info[YCSBTable::Secondary], key, sec_value)
            .unwrap();
    }
}

pub fn ycsb_load_all_tables(
    txn_storage: &impl TxnStorageTrait,
    config: &YCSBConfig,
) -> YCSBTableInfo {
    let mut table_info = YCSBTableInfo::new();
    let db_id = txn_storage.open_db(DBOptions::new("ycsb")).unwrap();
    assert_eq!(db_id, DB_ID);

    println!("======== Containers ========");
    let c_id = txn_storage
        .create_container(
            db_id,
            ContainerOptions::primary("primary", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(YCSBTable::Primary, c_id);
    println!("Primary container: {:?}", c_id);

    let c_id = txn_storage
        .create_container(
            db_id,
            ContainerOptions::secondary(
                "secondary",
                ContainerDS::BTree,
                table_info.map[&YCSBTable::Primary],
            ),
        )
        .unwrap();
    table_info.insert(YCSBTable::Secondary, c_id);
    println!("Secondary container: {:?}", c_id);

    load_primary_and_secondary_tables(txn_storage, &table_info, config);

    table_info
}

pub fn ycsb_show_table_stats(txn_storage: &impl TxnStorageTrait, table_info: &YCSBTableInfo) {
    // Show the stats by the order of c_id
    let mut table_info_ordered = table_info.map.iter().collect::<Vec<_>>();
    table_info_ordered.sort_by_key(|(_, c_id)| **c_id);
    for (table, c_id) in table_info_ordered {
        let stats = txn_storage.get_container_stats(DB_ID, *c_id).unwrap();
        println!("========= {:?} (c_id: {}) =========", table, c_id);
        println!("{}", stats);
    }
}

pub fn ycsb_preliminary_secondary_scan(
    txn_storage: &impl TxnStorageTrait,
    table_info: &YCSBTableInfo,
) {
    let txn = txn_storage.begin_txn(DB_ID, TxnOptions::default()).unwrap();
    let iter = txn_storage
        .scan_range(
            &txn,
            table_info[YCSBTable::Secondary],
            ScanOptions {
                lower: vec![],
                upper: vec![],
            },
        )
        .unwrap();
    let mut count = 0;
    while let Ok(Some((_key, _value))) = txn_storage.iter_next(&txn, &iter) {
        count += 1;
    }
    drop(iter);
    txn_storage.commit_txn(&txn, false).unwrap();
    println!("Secondary scan count: {}", count);
}
