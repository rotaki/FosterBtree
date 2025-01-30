use core::panic;
use std::{
    collections::{hash_map, HashMap},
    fmt::Display,
    sync::Mutex,
};

use dashmap::DashMap;

use crate::rwlatch::RwLatch;

pub struct ConcurrentLockTable {
    hashmap: DashMap<Vec<u8>, RwLatch>,
}

impl Display for ConcurrentLockTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keys = vec![];
        for key in self.hashmap.iter() {
            // Stringify the key
            let key_str = String::from_utf8_lossy(key.key());
            keys.push(key_str.to_string());
            let latch = key.value();
            writeln!(f, "Key: {}, Latch: {}", key_str, latch)?;
        }
        Ok(())
    }
}

impl Default for ConcurrentLockTable {
    fn default() -> Self {
        Self::new()
    }
}

impl ConcurrentLockTable {
    pub fn new() -> Self {
        ConcurrentLockTable {
            hashmap: DashMap::new(),
        }
    }

    pub fn try_shared(&self, key: Vec<u8>) -> bool {
        match self.hashmap.entry(key) {
            dashmap::Entry::Occupied(entry) => {
                let lock = entry.get();
                lock.try_shared()
            }
            dashmap::Entry::Vacant(entry) => {
                let lock = RwLatch::default();
                assert!(lock.try_shared());
                entry.insert(lock);
                true
            }
        }
    }

    pub fn try_exclusive(&self, key: Vec<u8>) -> bool {
        match self.hashmap.entry(key) {
            dashmap::Entry::Occupied(entry) => {
                let lock = entry.get();
                lock.try_exclusive()
            }
            dashmap::Entry::Vacant(entry) => {
                let lock = RwLatch::default();
                assert!(lock.try_exclusive());
                entry.insert(lock);
                true
            }
        }
    }

    pub fn try_upgrade(&self, key: Vec<u8>) -> bool {
        match self.hashmap.entry(key) {
            dashmap::Entry::Occupied(entry) => {
                let lock = entry.get();
                lock.try_upgrade()
            }
            dashmap::Entry::Vacant(_) => false,
        }
    }

    pub fn downgrade(&self, key: &[u8]) {
        let lock = self.hashmap.get(key).unwrap();
        lock.downgrade();
    }

    pub fn release_shared(&self, key: Vec<u8>) {
        match self.hashmap.entry(key) {
            dashmap::Entry::Occupied(entry) => {
                // While entry is exist, the shard is locked.
                // Hence no other thread can touch the entry.
                let lock = entry.get();
                lock.release_shared();
                if !lock.is_locked() {
                    entry.remove();
                }
            }
            dashmap::Entry::Vacant(_) => panic!("Lock not found"),
        }
    }

    pub fn release_exclusive(&self, key: Vec<u8>) {
        match self.hashmap.entry(key) {
            dashmap::Entry::Occupied(entry) => {
                // While entry is exist, the shard is locked.
                // Hence no other thread can touch the entry.
                let lock = entry.get();
                lock.release_exclusive();
                if !lock.is_locked() {
                    entry.remove();
                }
            }
            dashmap::Entry::Vacant(_) => panic!("Lock not found"),
        }
    }

    pub fn check_all_released(&self) -> bool {
        self.hashmap.is_empty()
    }

    pub fn check_state(&self, key: &[u8]) {
        if let Some(lock) = self.hashmap.get(key) {
            let latch = &*lock;
            println!("Key: {:?}, Lock: {:?}", key, latch);
        } else {
            println!("Key: {:?}, Lock: None", key);
        }
    }
}

#[allow(dead_code)]
pub struct SingleThreadLockTable {
    hashmap: Mutex<HashMap<Vec<u8>, RwLatch>>,
}

#[allow(dead_code)]
impl Display for SingleThreadLockTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keys = vec![];
        for (key, value) in self.hashmap.lock().unwrap().iter() {
            // Stringify the key
            let key_str = String::from_utf8_lossy(key);
            keys.push(key_str.to_string());
            writeln!(f, "Key: {}, Latch: {}", key_str, value)?;
        }
        Ok(())
    }
}

#[allow(dead_code)]
impl SingleThreadLockTable {
    pub fn new() -> Self {
        SingleThreadLockTable {
            hashmap: Mutex::new(HashMap::new()),
        }
    }

    pub fn try_shared(&self, key: Vec<u8>) -> bool {
        match self.hashmap.lock().unwrap().entry(key) {
            hash_map::Entry::Occupied(entry) => {
                let lock = entry.get();
                lock.try_shared()
            }
            hash_map::Entry::Vacant(entry) => {
                let lock = RwLatch::default();
                assert!(lock.try_shared());
                entry.insert(lock);
                true
            }
        }
    }

    pub fn try_exclusive(&self, key: Vec<u8>) -> bool {
        match self.hashmap.lock().unwrap().entry(key) {
            hash_map::Entry::Occupied(entry) => {
                let lock = entry.get();
                lock.try_exclusive()
            }
            hash_map::Entry::Vacant(entry) => {
                let lock = RwLatch::default();
                assert!(lock.try_exclusive());
                entry.insert(lock);
                true
            }
        }
    }

    pub fn try_upgrade(&self, key: Vec<u8>) -> bool {
        match self.hashmap.lock().unwrap().entry(key) {
            hash_map::Entry::Occupied(entry) => {
                let lock = entry.get();
                lock.try_upgrade()
            }
            hash_map::Entry::Vacant(_) => false,
        }
    }

    pub fn downgrade(&self, key: &[u8]) {
        if let Some(lock) = self.hashmap.lock().unwrap().get(key) {
            lock.downgrade();
        } else {
            panic!("Lock not found");
        }
    }

    pub fn release_shared(&self, key: Vec<u8>) {
        match self.hashmap.lock().unwrap().entry(key) {
            hash_map::Entry::Occupied(entry) => {
                // While entry is exist, the shard is locked.
                // Hence no other thread can touch the entry.
                let lock = entry.get();
                lock.release_shared();
                if !lock.is_locked() {
                    entry.remove();
                }
            }
            hash_map::Entry::Vacant(_) => panic!("Lock not found"),
        }
    }

    pub fn release_exclusive(&self, key: Vec<u8>) {
        match self.hashmap.lock().unwrap().entry(key) {
            hash_map::Entry::Occupied(entry) => {
                // While entry is exist, the shard is locked.
                // Hence no other thread can touch the entry.
                let lock = entry.get();
                lock.release_exclusive();
                if !lock.is_locked() {
                    entry.remove();
                }
            }
            hash_map::Entry::Vacant(_) => panic!("Lock not found"),
        }
    }

    pub fn check_all_released(&self) -> bool {
        self.hashmap.lock().unwrap().is_empty()
    }

    pub fn check_state(&self, key: &[u8]) {
        if let Some(lock) = self.hashmap.lock().unwrap().get(key) {
            let latch = lock;
            println!("Key: {:?}, Lock: {:?}", key, latch);
        } else {
            println!("Key: {:?}, Lock: None", key);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::random::small_thread_rng;

    use super::*;
    use rand::Rng;
    use std::collections::HashMap;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_shared_locks() {
        let lock_table = Arc::new(SingleThreadLockTable::new());
        let num_threads = 10;
        let key = b"test_key".to_vec();

        let barrier = Arc::new(Barrier::new(num_threads));

        let mut handles = vec![];

        for _ in 0..num_threads {
            let lock_table = Arc::clone(&lock_table);
            let barrier = Arc::clone(&barrier);
            let key = key.clone();
            let handle = thread::spawn(move || {
                assert!(lock_table.try_shared(key.clone()));
                barrier.wait();
                thread::sleep(Duration::from_millis(10));
                lock_table.release_shared(key.clone());
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_exclusive_lock() {
        let lock_table = Arc::new(SingleThreadLockTable::new());
        let key = b"test_key".to_vec();

        let lock_table1 = Arc::clone(&lock_table);
        let key1 = key.clone();
        let handle1 = thread::spawn(move || {
            assert!(lock_table1.try_exclusive(key1.clone()));
            thread::sleep(Duration::from_millis(50));
            lock_table1.release_exclusive(key1.clone());
        });

        thread::sleep(Duration::from_millis(10));

        let lock_table2 = Arc::clone(&lock_table);
        let key2 = key.clone();
        let handle2 = thread::spawn(move || {
            assert!(!lock_table2.try_shared(key2.clone()));
            assert!(!lock_table2.try_exclusive(key2.clone()));
            thread::sleep(Duration::from_millis(60));
            assert!(lock_table2.try_shared(key2.clone()));
            lock_table2.release_shared(key2.clone());
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
    }

    #[test]
    fn test_shared_then_exclusive() {
        let lock_table = Arc::new(SingleThreadLockTable::new());
        let key = b"test_key".to_vec();

        let num_shared_threads = 5;
        let barrier = Arc::new(Barrier::new(num_shared_threads + 1));

        let mut handles = vec![];

        for _ in 0..num_shared_threads {
            let lock_table = Arc::clone(&lock_table);
            let barrier = Arc::clone(&barrier);
            let key = key.clone();
            let handle = thread::spawn(move || {
                assert!(lock_table.try_shared(key.clone()));
                barrier.wait();
                thread::sleep(Duration::from_millis(50));
                lock_table.release_shared(key.clone());
            });
            handles.push(handle);
        }

        barrier.wait();

        let lock_table_exclusive = Arc::clone(&lock_table);
        let key_exclusive = key.clone();
        let handle_exclusive = thread::spawn(move || {
            assert!(!lock_table_exclusive.try_exclusive(key_exclusive.clone()));
            thread::sleep(Duration::from_millis(60));
            assert!(lock_table_exclusive.try_exclusive(key_exclusive.clone()));
            lock_table_exclusive.release_exclusive(key_exclusive.clone());
        });

        for handle in handles {
            handle.join().unwrap();
        }

        handle_exclusive.join().unwrap();
    }

    #[test]
    fn test_try_upgrade() {
        let lock_table = Arc::new(SingleThreadLockTable::new());
        let key = b"test_key".to_vec();

        let lock_table_shared = Arc::clone(&lock_table);
        let key_shared = key.clone();
        let handle_shared = thread::spawn(move || {
            assert!(lock_table_shared.try_shared(key_shared.clone()));
            thread::sleep(Duration::from_millis(100));
            lock_table_shared.release_shared(key_shared.clone());
        });

        let lock_table_upgrade = Arc::clone(&lock_table);
        let key_upgrade = key.clone();
        let handle_upgrade = thread::spawn(move || {
            assert!(lock_table_upgrade.try_shared(key_upgrade.clone()));
            thread::sleep(Duration::from_millis(50));
            assert!(!lock_table_upgrade.try_upgrade(key_upgrade.clone()));
            thread::sleep(Duration::from_millis(60));
            assert!(lock_table_upgrade.try_upgrade(key_upgrade.clone()));
            lock_table_upgrade.release_exclusive(key_upgrade.clone());
        });

        handle_shared.join().unwrap();
        handle_upgrade.join().unwrap();
    }

    #[test]
    fn test_lock_cleanup() {
        let lock_table = SingleThreadLockTable::new();
        let key = b"test_key".to_vec();

        assert!(lock_table.try_shared(key.clone()));
        lock_table.release_shared(key.clone());

        assert!(lock_table.try_shared(key.clone()));
        lock_table.release_shared(key.clone());
    }

    #[test]
    fn stress_test_lock_table_with_state() {
        let lock_table = Arc::new(SingleThreadLockTable::new());
        let num_threads = 100;
        let keys: Vec<Vec<u8>> = (0..10).map(|i| format!("key_{}", i).into_bytes()).collect();

        let mut handles = vec![];

        for _ in 0..num_threads {
            let lock_table = Arc::clone(&lock_table);
            let keys = keys.clone();
            let handle = thread::spawn(move || {
                let mut rng = small_thread_rng();
                let mut held_shared_locks: HashMap<Vec<u8>, bool> = HashMap::new();
                let mut held_exclusive_locks: HashMap<Vec<u8>, bool> = HashMap::new();

                for _ in 0..100 {
                    let key = keys[rng.random_range(0..keys.len())].clone();
                    let op = rng.random_range(0..5);
                    match op {
                        0 => {
                            if lock_table.try_shared(key.clone()) {
                                held_shared_locks.insert(key.clone(), true);
                            }
                        }
                        1 => {
                            if lock_table.try_exclusive(key.clone()) {
                                held_exclusive_locks.insert(key.clone(), true);
                            }
                        }
                        2 => {
                            if held_shared_locks.remove(&key).is_some() {
                                lock_table.release_shared(key.clone());
                            }
                        }
                        3 => {
                            if held_exclusive_locks.remove(&key).is_some() {
                                lock_table.release_exclusive(key.clone());
                            }
                        }
                        4 => {
                            if held_shared_locks.contains_key(&key)
                                && lock_table.try_upgrade(key.clone())
                            {
                                held_shared_locks.remove(&key);
                                held_exclusive_locks.insert(key.clone(), true);
                            }
                        }
                        _ => {}
                    }
                    thread::sleep(Duration::from_millis(rng.random_range(0..5)));
                }

                for key in held_shared_locks.keys() {
                    lock_table.release_shared(key.clone());
                }
                for key in held_exclusive_locks.keys() {
                    lock_table.release_exclusive(key.clone());
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_display() {
        let lock_table = SingleThreadLockTable::new();
        let key1 = b"key1".to_vec();
        let key2 = b"key2".to_vec();

        lock_table.try_shared(key1.clone());
        lock_table.try_exclusive(key2.clone());
        lock_table.try_shared(key1.clone());

        let display = format!("{}", lock_table);
        println!("{}", display);
    }
}
