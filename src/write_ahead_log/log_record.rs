#[derive(Debug, PartialEq)]
pub enum LogRecord<'a> {
    // User transaction operations
    TxnBegin {
        txn_id: u64,
    },
    TxnCommit {
        txn_id: u64,
    },
    TxnAbort {
        txn_id: u64,
    },
    PageInsert {
        txn_id: u64,
        page_id: u32,
        key: &'a [u8],
        value: &'a [u8],
    },
    PageUpdate {
        txn_id: u64,
        page_id: u32,
        key: &'a [u8],
        before_value: &'a [u8],
        after_value: &'a [u8],
    },
    PageDelete {
        txn_id: u64,
        page_id: u32,
        key: &'a [u8],
        value: &'a [u8],
    },

    // System transaction operations
    SysTxnAllocPage {
        txn_id: u64,
        page_id: u32,
    },
    SysTxnSplit {
        txn_id: u64,
        page_id: u32,
        new_page_id: u32,
        key_values: Vec<(&'a [u8], &'a [u8])>,
    },
}

impl LogRecord<'_> {
    pub fn get_page_ids(&self) -> Vec<u32> {
        match self {
            LogRecord::PageInsert { page_id, .. } => vec![*page_id],
            LogRecord::PageUpdate { page_id, .. } => vec![*page_id],
            LogRecord::PageDelete { page_id, .. } => vec![*page_id],
            LogRecord::SysTxnAllocPage { page_id, .. } => vec![*page_id],
            LogRecord::SysTxnSplit {
                page_id,
                new_page_id,
                ..
            } => vec![*page_id, *new_page_id],
            _ => vec![],
        }
    }

    // Serialize the records to bytes without using bincode
    pub fn to_bytes(&self) -> Vec<u8> {
        match &self {
            LogRecord::TxnBegin { txn_id } => {
                let mut bytes = vec![0];
                bytes.extend_from_slice(&txn_id.to_be_bytes());
                bytes
            }
            LogRecord::TxnCommit { txn_id } => {
                let mut bytes = vec![1];
                bytes.extend_from_slice(&txn_id.to_be_bytes());
                bytes
            }
            LogRecord::TxnAbort { txn_id } => {
                let mut bytes = vec![2];
                bytes.extend_from_slice(&txn_id.to_be_bytes());
                bytes
            }
            LogRecord::PageInsert {
                txn_id,
                page_id,
                key,
                value,
            } => {
                let mut bytes = vec![3];
                bytes.extend_from_slice(&txn_id.to_be_bytes());
                bytes.extend_from_slice(&page_id.to_be_bytes());
                bytes.extend_from_slice(&(key.len() as u32).to_be_bytes());
                bytes.extend_from_slice(key);
                bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
                bytes.extend_from_slice(value);
                bytes
            }
            LogRecord::PageUpdate {
                txn_id,
                page_id,
                key,
                before_value,
                after_value,
            } => {
                let mut bytes = vec![4];
                bytes.extend_from_slice(&txn_id.to_be_bytes());
                bytes.extend_from_slice(&page_id.to_be_bytes());
                bytes.extend_from_slice(&(key.len() as u32).to_be_bytes());
                bytes.extend_from_slice(key);
                bytes.extend_from_slice(&(before_value.len() as u32).to_be_bytes());
                bytes.extend_from_slice(before_value);
                bytes.extend_from_slice(&(after_value.len() as u32).to_be_bytes());
                bytes.extend_from_slice(after_value);
                bytes
            }
            LogRecord::PageDelete {
                txn_id,
                page_id,
                key,
                value,
            } => {
                let mut bytes = vec![5];
                bytes.extend_from_slice(&txn_id.to_be_bytes());
                bytes.extend_from_slice(&page_id.to_be_bytes());
                bytes.extend_from_slice(&(key.len() as u32).to_be_bytes());
                bytes.extend_from_slice(key);
                bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
                bytes.extend_from_slice(value);
                bytes
            }
            LogRecord::SysTxnAllocPage { txn_id, page_id } => {
                let mut bytes = vec![6];
                bytes.extend_from_slice(&txn_id.to_be_bytes());
                bytes.extend_from_slice(&page_id.to_be_bytes());
                bytes
            }
            LogRecord::SysTxnSplit {
                txn_id,
                page_id,
                new_page_id,
                key_values,
            } => {
                let mut bytes = vec![7];
                bytes.extend_from_slice(&txn_id.to_be_bytes());
                bytes.extend_from_slice(&page_id.to_be_bytes());
                bytes.extend_from_slice(&new_page_id.to_be_bytes());
                bytes.extend_from_slice(&(key_values.len() as u32).to_be_bytes());
                for (key, value) in key_values {
                    bytes.extend_from_slice(&(key.len() as u32).to_be_bytes());
                    bytes.extend_from_slice(key);
                    bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
                    bytes.extend_from_slice(value);
                }
                bytes
            }
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> LogRecord {
        let mut bytes = bytes;
        let record_type = bytes[0];
        bytes = &bytes[1..];
        match record_type {
            0 => {
                let txn_id = u64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                LogRecord::TxnBegin { txn_id }
            }
            1 => {
                let txn_id = u64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                LogRecord::TxnCommit { txn_id }
            }
            2 => {
                let txn_id = u64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                LogRecord::TxnAbort { txn_id }
            }
            3 => {
                let txn_id = u64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                bytes = &bytes[8..];
                let page_id = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                bytes = &bytes[4..];
                let key_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                bytes = &bytes[4..];
                let key = &bytes[..key_len as usize];
                bytes = &bytes[key_len as usize..];
                let value_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                bytes = &bytes[4..];
                let value = &bytes[..value_len as usize];
                LogRecord::PageInsert {
                    txn_id,
                    page_id,
                    key,
                    value,
                }
            }
            4 => {
                let txn_id = u64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                bytes = &bytes[8..];
                let page_id = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                bytes = &bytes[4..];
                let key_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                bytes = &bytes[4..];
                let key = &bytes[..key_len as usize];
                bytes = &bytes[key_len as usize..];
                let before_value_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                bytes = &bytes[4..];
                let before_value = &bytes[..before_value_len as usize];
                bytes = &bytes[before_value_len as usize..];
                let after_value_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                bytes = &bytes[4..];
                let after_value = &bytes[..after_value_len as usize];
                LogRecord::PageUpdate {
                    txn_id,
                    page_id,
                    key,
                    before_value,
                    after_value,
                }
            }
            5 => {
                let txn_id = u64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                bytes = &bytes[8..];
                let page_id = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                bytes = &bytes[4..];
                let key_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                bytes = &bytes[4..];
                let key = &bytes[..key_len as usize];
                bytes = &bytes[key_len as usize..];
                let value_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                bytes = &bytes[4..];
                let value = &bytes[..value_len as usize];
                LogRecord::PageDelete {
                    txn_id,
                    page_id,
                    key,
                    value,
                }
            }
            6 => {
                let txn_id = u64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                bytes = &bytes[8..];
                let page_id = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                LogRecord::SysTxnAllocPage { txn_id, page_id }
            }
            7 => {
                let txn_id = u64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                bytes = &bytes[8..];
                let page_id = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                bytes = &bytes[4..];
                let new_page_id = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                bytes = &bytes[4..];
                let mut key_values = vec![];
                let key_values_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                bytes = &bytes[4..];
                for _ in 0..key_values_len {
                    let key_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    bytes = &bytes[4..];
                    let key = &bytes[..key_len as usize];
                    bytes = &bytes[key_len as usize..];
                    let value_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    bytes = &bytes[4..];
                    let value = &bytes[..value_len as usize];
                    bytes = &bytes[value_len as usize..];
                    key_values.push((key, value));
                }
                LogRecord::SysTxnSplit {
                    txn_id,
                    page_id,
                    new_page_id,
                    key_values,
                }
            }
            _ => panic!("Invalid log record type"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn test_txn_begin_to_bytes_and_back() {
        let record = LogRecord::TxnBegin { txn_id: 123 };
        let bytes = record.to_bytes();
        let decoded = LogRecord::from_bytes(&bytes);
        assert_eq!(decoded, record);
    }

    #[test]
    #[ignore]
    fn test_txn_commit_to_bytes_and_back() {
        let record = LogRecord::TxnCommit { txn_id: 123 };
        let bytes = record.to_bytes();
        let decoded = LogRecord::from_bytes(&bytes);
        assert_eq!(decoded, record);
    }

    #[test]
    #[ignore]
    fn test_txn_abort_to_bytes_and_back() {
        let record = LogRecord::TxnAbort { txn_id: 123 };
        let bytes = record.to_bytes();
        let decoded = LogRecord::from_bytes(&bytes);
        assert_eq!(decoded, record);
    }

    #[test]
    #[ignore]
    fn test_page_insert_to_bytes_and_back() {
        let key = b"key";
        let value = b"value";
        let record = LogRecord::PageInsert {
            txn_id: 123,
            page_id: 1,
            key,
            value,
        };
        let bytes = record.to_bytes();
        let decoded = LogRecord::from_bytes(&bytes);
        assert_eq!(decoded, record);
    }

    #[test]
    #[ignore]
    fn test_page_update_to_bytes_and_back() {
        let record = LogRecord::PageUpdate {
            txn_id: 123,
            page_id: 1,
            key: b"key",
            before_value: b"old_value",
            after_value: b"new_value",
        };
        let bytes = record.to_bytes();
        let decoded = LogRecord::from_bytes(&bytes);
        assert_eq!(decoded, record);
    }

    #[test]
    #[ignore]
    fn test_page_delete_to_bytes_and_back() {
        let record = LogRecord::PageDelete {
            txn_id: 123,
            page_id: 1,
            key: b"key",
            value: b"value",
        };
        let bytes = record.to_bytes();
        let decoded = LogRecord::from_bytes(&bytes);
        assert_eq!(decoded, record);
    }

    #[test]
    #[ignore]
    fn test_sys_txn_alloc_page_to_bytes_and_back() {
        let record = LogRecord::SysTxnAllocPage {
            txn_id: 123,
            page_id: 1,
        };
        let bytes = record.to_bytes();
        let decoded = LogRecord::from_bytes(&bytes);
        assert_eq!(decoded, record);
    }

    #[test]
    #[ignore]
    fn test_sys_txn_split_to_bytes_and_back() {
        let key_values: Vec<(&[u8], &[u8])> = vec![(b"key", b"value"), (b"key2", b"value2")];
        let record = LogRecord::SysTxnSplit {
            txn_id: 123,
            page_id: 1,
            new_page_id: 2,
            key_values,
        };
        let bytes = record.to_bytes();
        let decoded = LogRecord::from_bytes(&bytes);
        assert_eq!(decoded, record);
    }

    #[test]
    #[ignore]
    fn test_get_page_ids() {
        let record = LogRecord::PageUpdate {
            txn_id: 123,
            page_id: 1,
            key: b"key",
            before_value: b"old_value",
            after_value: b"new_value",
        };
        assert_eq!(record.get_page_ids(), vec![1]);

        let record = LogRecord::SysTxnSplit {
            txn_id: 123,
            page_id: 1,
            new_page_id: 2,
            key_values: vec![(b"key", b"value")],
        };
        assert_eq!(record.get_page_ids(), vec![1, 2]);
    }
}
