#[cfg(target_arch = "wasm32")]
pub mod inner {
    use wasm_bindgen::prelude::*;

    use crate::bp::prelude::*;
    use crate::fbt::foster_btree::{deserialize_page_id, FosterBtree, PageVisitor, MAX_BYTES_USED};
    use crate::fbt::FosterBtreePage;
    use crate::page::{Page, PageId};

    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    #[wasm_bindgen]
    pub struct FosterBtreeVisualizer {
        bt: FosterBtree<DummyEvictionPolicy, InMemPool<DummyEvictionPolicy>>,
    }

    #[wasm_bindgen]
    impl FosterBtreeVisualizer {
        #[wasm_bindgen(constructor)]
        pub fn new() -> FosterBtreeVisualizer {
            let (db_id, c_id) = (0, 0);
            let c_key = ContainerKey::new(db_id, c_id);
            let mem_pool = Arc::new(InMemPool::new());
            let btree = FosterBtree::new(c_key, mem_pool);
            FosterBtreeVisualizer { bt: btree }
        }

        pub fn insert(&mut self, key: usize, payload_size: usize) -> String {
            if payload_size > MAX_BYTES_USED {
                return "Payload size is too large".to_string();
            }
            match self.bt.insert(&key.to_be_bytes(), &vec![1; payload_size]) {
                Ok(_) => {
                    format!("Inserted key: {}", key)
                }
                Err(e) => {
                    format!("Error: {:?}", e)
                }
            }
        }

        pub fn update(&mut self, key: usize, payload_size: usize) -> String {
            if payload_size > MAX_BYTES_USED {
                return "Payload size is too large".to_string();
            }
            match self.bt.update(&key.to_be_bytes(), &vec![1; payload_size]) {
                Ok(_) => {
                    format!("Updated key: {}", key)
                }
                Err(e) => {
                    format!("Error: {:?}", e)
                }
            }
        }

        pub fn delete(&mut self, key: usize) -> String {
            match self.bt.delete(&key.to_be_bytes()) {
                Ok(_) => {
                    format!("Deleted key: {}", key)
                }
                Err(e) => {
                    format!("Error: {:?}", e)
                }
            }
        }

        pub fn upsert(&mut self, key: usize, payload_size: usize) -> String {
            if payload_size > MAX_BYTES_USED {
                return "Payload size is too large".to_string();
            }
            match self.bt.upsert(&key.to_be_bytes(), &vec![1; payload_size]) {
                Ok(_) => {
                    format!("Upserted key: {}", key)
                }
                Err(e) => {
                    format!("Error: {:?}", e)
                }
            }
        }

        pub fn get(&self, key: usize) -> String {
            match self.bt.get(&key.to_be_bytes()) {
                Ok(val) => {
                    format!("Found key: {}, payload_size: {}", key, val.len())
                }
                Err(e) => {
                    format!("Error: {:?}", e)
                }
            }
        }

        pub fn visualize(&self) -> String {
            let mut dot_gen = DotGraphGenerator::new();
            let page_traverser = self.bt.page_traverser();
            page_traverser.visit(&mut dot_gen);
            dot_gen.to_string()
        }
    }

    pub struct DotGraphGenerator {
        dot: String,
        levels: HashMap<u8, HashSet<PageId>>,
    }

    impl DotGraphGenerator {
        fn new() -> Self {
            let mut dot = "digraph G {\n".to_string();
            // rectangle nodes
            dot.push_str("\tnode [shape=rectangle];\n");
            Self {
                dot,
                levels: HashMap::new(),
            }
        }

        fn page_to_dot_string(&self, page: &Page) -> String {
            let mut result = format!("\t{} [label=\"[P{}](", page.get_id(), page.get_id());
            // If page is a leaf, add the key-value pairs
            let low_fence = page.get_raw_key(page.low_fence_slot_id());
            if low_fence.is_empty() {
                result.push_str("L[-∞], ");
            } else {
                let low_fence_usize = usize::from_be_bytes(low_fence.try_into().unwrap());
                result.push_str(&format!("L[{}], ", low_fence_usize));
            }
            for i in 1..=page.active_slot_count() {
                let key = page.get_raw_key(i);
                let key = if key.is_empty() {
                    "-∞".to_string()
                } else {
                    let key_usize = usize::from_be_bytes(key.try_into().unwrap());
                    key_usize.to_string()
                };
                result.push_str(&format!("{}, ", key));
            }
            let high_fence = page.get_raw_key(page.high_fence_slot_id());
            if high_fence.is_empty() {
                result.push_str("H[+∞])\"];");
            } else {
                let high_fence_usize = usize::from_be_bytes(high_fence.try_into().unwrap());
                result.push_str(&format!("H[{}])\"];", high_fence_usize));
            }
            result.push_str("\n");
            result
        }

        fn to_string(&self) -> String {
            let mut result = self.dot.clone();
            for (_, page_ids) in self.levels.iter() {
                result.push_str(&format!("\t{{rank=same; {:?}}}\n", page_ids));
            }
            result.push_str("}\n");
            result
        }
    }

    impl PageVisitor for DotGraphGenerator {
        fn visit_pre(&mut self, page: &Page) {
            self.dot.push_str(&self.page_to_dot_string(page));

            let level = page.level();
            let id = page.get_id();

            self.levels
                .entry(level)
                .or_insert(HashSet::new())
                .insert(id);

            if page.is_leaf() {
                if page.has_foster_child() {
                    let foster_child_page_id =
                        deserialize_page_id(page.get_foster_val().try_into().unwrap()).unwrap();
                    self.dot
                        .push_str(&format!("\t{} -> {};\n", id, foster_child_page_id));
                }
            } else {
                for i in 1..=page.active_slot_count() {
                    let child_page_id =
                        deserialize_page_id(page.get_val(i).try_into().unwrap()).unwrap();
                    self.dot
                        .push_str(&format!("\t{} -> {};\n", id, child_page_id));
                }
            }
        }

        fn visit_post(&mut self, page: &Page) {}
    }
}
