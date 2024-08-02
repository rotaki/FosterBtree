use crate::bp::ContainerId;
use crate::file_manager::FileManager;
use crate::heap_page::HeapPage;
use crate::page::Page;
use std::sync::Mutex;

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

struct LogBufferInner {
    current_page_idx: usize,
    buffer: Vec<Page>,
    file_manager: FileManager,
}

impl LogBufferInner {
    fn new<P: AsRef<std::path::Path>>(db_path: P, c_id: ContainerId, num_pages: usize) -> Self {
        let file_manager = FileManager::new(db_path, c_id).unwrap();
        let mut buffer = Vec::with_capacity(num_pages);
        for _ in 0..num_pages {
            let page_id = file_manager.fetch_add_page_id();
            let mut page = Page::new_empty();
            HeapPage::init(&mut page);
            page.set_id(page_id);
            buffer.push(page);
        }

        LogBufferInner {
            current_page_idx: 0,
            buffer,
            file_manager,
        }
    }

    fn append_log(&mut self, log_record: &[u8]) -> Lsn {
        loop {
            let page = &mut self.buffer[self.current_page_idx];
            let mut h_page = HeapPage::new(page);
            if let Some(slot_id) = h_page.add_value(log_record) {
                return Lsn::new(page.get_id(), slot_id);
            } else {
                self.current_page_idx += 1;
                if self.current_page_idx == self.buffer.len() {
                    self.flush_all();
                    self.current_page_idx = 0;
                }
            }
        }
    }

    /// Ensure all logs less than or equal to `lsn` are flushed to disk.
    /// Currently, we just flush all logs to disk if `lsn` is greater or equal
    /// to the first log in the buffer.
    #[allow(dead_code)]
    fn flush_to(&mut self, lsn: &Lsn) {
        // Check if the log is in the range of the buffer
        let min_page_id = self.buffer[0].get_id();
        if lsn.page_id >= min_page_id {
            self.flush_all();
        }
    }

    fn flush_all(&mut self) {
        for page in &mut self.buffer {
            self.file_manager.write_page(page.get_id(), page).unwrap();
            HeapPage::init(page);
            let new_page_id = self.file_manager.fetch_add_page_id();
            page.set_id(new_page_id);
        }
        self.file_manager.flush().unwrap();
    }
}

// A very simple LogBuffer that just appends log records to a buffer and then flushes them to disk
// when the buffer is full.
pub struct LogBuffer {
    inner: Mutex<LogBufferInner>,
}

impl LogBuffer {
    pub fn new<P: AsRef<std::path::Path>>(db_path: P, c_id: ContainerId, num_pages: usize) -> Self {
        LogBuffer {
            inner: Mutex::new(LogBufferInner::new(db_path, c_id, num_pages)),
        }
    }

    pub fn append_log(&self, log_record: &[u8]) -> Lsn {
        self.inner.lock().unwrap().append_log(log_record)
    }

    pub fn flush_all(&self) {
        self.inner.lock().unwrap().flush_all();
    }
}

#[cfg(test)]
pub struct LogChecker {
    file_manager: FileManager,
}

#[cfg(test)]
impl LogChecker {
    pub fn new<P: AsRef<std::path::Path>>(db_path: P, c_id: ContainerId) -> Self {
        LogChecker {
            file_manager: FileManager::new(db_path, c_id).unwrap(),
        }
    }

    pub fn get_all_logs(&self) -> Vec<Vec<u8>> {
        let mut logs = Vec::new();
        let num_pages = self.file_manager.fetch_add_page_id();
        for page_id in 0..num_pages {
            let mut page = Page::new_empty();
            self.file_manager.read_page(page_id, &mut page).unwrap();
            let h_page = HeapPage::new(&mut page);
            for (_, rec) in h_page.get_all_valid_records() {
                logs.push(rec.to_owned());
            }
        }
        logs
    }

    pub fn get_log(&self, lsn: &Lsn) -> Vec<u8> {
        let mut page = Page::new_empty();
        self.file_manager.read_page(lsn.page_id, &mut page).unwrap();
        let h_page = HeapPage::new(&mut page);
        h_page.get_value(lsn.slot_id).unwrap().to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::random::gen_random_byte_vec;
    use tempfile::tempdir;

    #[test]
    #[ignore]
    fn test_append_log_and_flush() {
        let dir = tempdir().unwrap();
        let log_buffer = LogBuffer::new(&dir, 0, 2);

        // generate a bunch of random vecs and append them to the log buffer
        let mut lsn_vec = Vec::new();
        let mut logs = Vec::new();
        for _ in 0..1000 {
            // generate len (min_len 50, max_len 100) using random range
            let byte_vec = gen_random_byte_vec(50, 100);
            let lsn = log_buffer.append_log(&byte_vec);
            logs.push(byte_vec);
            lsn_vec.push(lsn);
        }
        log_buffer.flush_all();

        // check if the logs are written correctly
        let log_checker = LogChecker::new(&dir, 0);
        for (i, lsn) in lsn_vec.iter().enumerate() {
            assert_eq!(logs[i], log_checker.get_log(lsn));
        }
        assert_eq!(logs, log_checker.get_all_logs());
    }
}
