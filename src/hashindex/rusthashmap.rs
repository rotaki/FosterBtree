use std::collections::HashMap;
use std::sync::{Mutex, RwLock};

pub struct RustHashMap {
    // func: Box<dyn Fn(&[u8], &[u8]) -> Vec<u8>>,
    pub hm: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl RustHashMap {
    pub fn new() -> Self {
        RustHashMap {
            // func,
            hm: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>> {
        let mut hm = self.hm.write().unwrap();
        match hm.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => Some(entry.insert(value)),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(value);
                None
            }
        }
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        self.hm.read().unwrap().get(key).cloned()
    }

    pub fn remove(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        self.hm.write().unwrap().remove(key)
    }
}
