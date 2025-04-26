use super::mem_pool_trait::PageFrameKey;
use super::{
    eviction_policy::{EvictionPolicy, LRUEvictionPolicy},
    mem_pool_trait::PageKey,
};
use crate::page::Page;
use crate::rwlatch::RwLatch;
use std::{
    cell::UnsafeCell,
    fmt::Debug,
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::atomic::{AtomicBool, Ordering},
};

type EvictionPolicyType = LRUEvictionPolicy;

#[repr(C, align(256))]
pub struct FrameMeta {
    frame_id: u32, // An index of the frame in the buffer pool. This is a constant value.
    latch: RwLatch,
    is_dirty: AtomicBool, // Can be updated even when ReadGuard is held (see flush_all() in buffer_pool.rs)
    evict_info: EvictionPolicyType, // Can be updated even when ReadGuard is held (see get_page_for_read() in buffer_pool.rs). Interior mutability must be used.
    key: UnsafeCell<Option<PageKey>>, // Can only be updated when WriteGuard is held
}

impl FrameMeta {
    pub fn new(frame_id: u32) -> Self {
        FrameMeta {
            frame_id,
            latch: RwLatch::default(),
            is_dirty: AtomicBool::new(false),
            evict_info: EvictionPolicyType::new(),
            key: UnsafeCell::new(None),
        }
    }
}

unsafe impl Send for FrameMeta {}
unsafe impl Sync for FrameMeta {}

pub struct FrameReadGuard {
    upgraded: AtomicBool,
    meta: NonNull<FrameMeta>,
    page: NonNull<Page>,
    _marker: std::marker::PhantomData<*mut ()>,
}

unsafe impl Send for FrameReadGuard {}
// I don't think we need sync for FrameReadGuard, because it is not shared between threads.

impl FrameReadGuard {
    pub fn new(meta: *mut FrameMeta, page: *mut Page) -> Self {
        let upgraded = AtomicBool::new(false);
        let meta = NonNull::new(meta).expect("Meta pointer is null");
        let page = NonNull::new(page).expect("Page pointer is null");
        unsafe { meta.as_ref().latch.shared() };
        FrameReadGuard {
            upgraded,
            meta,
            page,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn try_new(meta: *mut FrameMeta, page: *mut Page) -> Option<Self> {
        let upgraded = AtomicBool::new(false);
        let meta = NonNull::new(meta).expect("Meta pointer is null");
        let page = NonNull::new(page).expect("Page pointer is null");
        if unsafe { meta.as_ref().latch.try_shared() } {
            Some(FrameReadGuard {
                upgraded,
                meta,
                page,
                _marker: std::marker::PhantomData,
            })
        } else {
            None
        }
    }

    pub fn frame_id(&self) -> u32 {
        // SAFETY: This is safe because frame meta must be a valid pointer and frame_id is not mutable.
        unsafe { self.meta.as_ref().frame_id }
    }

    pub fn latch(&self) -> &RwLatch {
        // SAFETY: This is safe because frame meta must be a valid pointer.
        unsafe { &self.meta.as_ref().latch }
    }

    pub fn dirty(&self) -> &AtomicBool {
        // SAFETY: This is safe because frame meta must be a valid pointer.
        unsafe { &self.meta.as_ref().is_dirty }
    }

    pub fn evict_info(&self) -> &EvictionPolicyType {
        // SAFETY: This is safe because frame meta must be a valid pointer.
        unsafe { &self.meta.as_ref().evict_info }
    }

    pub fn page_key(&self) -> &Option<PageKey> {
        // SAFETY: This is safe because frame meta must be a valid pointer.
        unsafe { &*self.meta.as_ref().key.get() }
    }

    pub fn page_frame_key(&self) -> Option<PageFrameKey> {
        self.page_key().map(|p_key| {
            PageFrameKey::new_with_frame_id(p_key.c_key, p_key.page_id, self.frame_id())
        })
    }

    pub fn page(&self) -> &Page {
        // SAFETY: This is safe because frame meta must be a valid pointer and we have a shared latch.
        unsafe { self.page.as_ref() }
    }

    pub fn try_upgrade(self, make_dirty: bool) -> Result<FrameWriteGuard, FrameReadGuard> {
        if self.latch().try_upgrade() {
            self.upgraded.store(true, Ordering::Relaxed);
            if make_dirty {
                self.dirty().store(true, Ordering::Release);
            }
            Ok(FrameWriteGuard {
                downgraded: AtomicBool::new(false),
                meta: self.meta,
                page: self.page,
                _marker: std::marker::PhantomData,
            })
        } else {
            Err(self)
        }
    }
}

impl Drop for FrameReadGuard {
    fn drop(&mut self) {
        if !self.upgraded.load(Ordering::Relaxed) {
            self.latch().release_shared();
        }
    }
}

impl Deref for FrameReadGuard {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.page()
    }
}

impl Debug for FrameReadGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameReadGuard")
            .field("key", &self.page_key())
            .field("dirty", &self.dirty().load(Ordering::Relaxed))
            .finish()
    }
}

pub struct FrameWriteGuard {
    downgraded: AtomicBool,
    meta: NonNull<FrameMeta>,
    page: NonNull<Page>,
    _marker: std::marker::PhantomData<*mut ()>,
}

impl FrameWriteGuard {
    pub fn new(meta: *mut FrameMeta, page: *mut Page, make_dirty: bool) -> Self {
        let downgraded = AtomicBool::new(false);
        let meta = NonNull::new(meta).expect("Meta pointer is null");
        let page = NonNull::new(page).expect("Page pointer is null");
        unsafe { meta.as_ref().latch.exclusive() };
        if make_dirty {
            unsafe {
                meta.as_ref().is_dirty.store(true, Ordering::Release);
            }
        }
        FrameWriteGuard {
            downgraded,
            meta,
            page,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn try_new(meta: *mut FrameMeta, page: *mut Page, make_dirty: bool) -> Option<Self> {
        let downgraded = AtomicBool::new(false);
        let meta = NonNull::new(meta).expect("Meta pointer is null");
        let page = NonNull::new(page).expect("Page pointer is null");
        if unsafe { meta.as_ref().latch.try_exclusive() } {
            if make_dirty {
                unsafe {
                    meta.as_ref().is_dirty.store(true, Ordering::Release);
                }
            }
            Some(FrameWriteGuard {
                downgraded,
                meta,
                page,
                _marker: std::marker::PhantomData,
            })
        } else {
            None
        }
    }

    pub fn frame_id(&self) -> u32 {
        // SAFETY: This is safe because frame meta must be a valid pointer and frame_id is not mutable.
        unsafe { self.meta.as_ref().frame_id }
    }

    pub fn latch(&self) -> &RwLatch {
        // SAFETY: This is safe because frame meta must be a valid pointer.
        unsafe { &self.meta.as_ref().latch }
    }

    pub fn dirty(&self) -> &AtomicBool {
        // SAFETY: This is safe because frame meta must be a valid pointer.
        unsafe { &self.meta.as_ref().is_dirty }
    }

    pub fn evict_info(&self) -> &EvictionPolicyType {
        // SAFETY: This is safe because frame meta must be a valid pointer.
        unsafe { &self.meta.as_ref().evict_info }
    }

    pub fn page_key(&self) -> &Option<PageKey> {
        // SAFETY: This is safe because frame meta must be a valid pointer.
        unsafe { &*self.meta.as_ref().key.get() }
    }

    pub fn page_key_mut(&mut self) -> &mut Option<PageKey> {
        // SAFETY: This is safe because frame meta must be a valid pointer.
        unsafe { &mut *self.meta.as_ref().key.get() }
    }

    pub fn page_frame_key(&self) -> Option<PageFrameKey> {
        self.page_key().map(|p_key| {
            PageFrameKey::new_with_frame_id(p_key.c_key, p_key.page_id, self.frame_id())
        })
    }

    pub fn page(&self) -> &Page {
        // SAFETY: This is safe because frame meta must be a valid pointer and we have a shared latch.
        unsafe { self.page.as_ref() }
    }

    pub fn page_mut(&mut self) -> &mut Page {
        // SAFETY: This is safe because frame meta must be a valid pointer and we have a shared latch.
        unsafe { &mut *self.page.as_ptr() }
    }

    pub fn downgrade(self) -> FrameReadGuard {
        self.latch().downgrade();
        self.downgraded.store(true, Ordering::Relaxed);
        FrameReadGuard {
            upgraded: AtomicBool::new(false),
            meta: self.meta,
            page: self.page,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn clear(&mut self) {
        self.dirty().store(false, Ordering::Release);
        self.evict_info().reset();
        self.page_key_mut().take();
    }
}

impl Drop for FrameWriteGuard {
    fn drop(&mut self) {
        if !self.downgraded.load(Ordering::Relaxed) {
            self.latch().release_exclusive();
        }
    }
}

impl Deref for FrameWriteGuard {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        // SAFETY: This is safe because the latch is held exclusively.
        self.page()
    }
}

impl DerefMut for FrameWriteGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: This is safe because the latch is held exclusively.
        self.page_mut()
    }
}

impl Debug for FrameWriteGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameWriteGuard")
            .field("key", &self.page_key())
            .field("dirty", &self.dirty().load(Ordering::Relaxed))
            .finish()
    }
}

pub fn box_as_mut_ptr<T>(b: &mut Box<T>) -> *mut T {
    // This is a primitive deref, not going through `DerefMut`, and therefore not materializing
    // any references.
    // See Box::as_mut_ptr in the standard library. We do not use it here because it is
    // not in stable Rust yet.
    &raw mut **b
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::thread;

    fn make_meta_and_page(frame_id: usize) -> (Box<FrameMeta>, Box<Page>) {
        let page = Box::new(Page::new_empty());
        let meta = Box::new(FrameMeta::new(frame_id as u32));
        (meta, page)
    }

    #[test]
    fn test_default_buffer_frame() {
        let (mut meta, mut page) = make_meta_and_page(0);
        let guard = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        assert!(!guard.dirty().load(Ordering::Relaxed));
        assert!(guard.page_key().is_none());
    }

    #[test]
    fn test_read_access() {
        let (mut meta, mut page) = make_meta_and_page(0);
        let guard = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        assert_eq!(guard.page_key(), &None);
        assert!(!guard.dirty().load(Ordering::Relaxed));
        guard.iter().all(|&x| x == 0);
        assert!(!guard.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_write_access() {
        let (mut meta, mut page) = make_meta_and_page(0);
        let mut guard =
            FrameWriteGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page), true);
        assert_eq!(guard.page_key(), &None);
        assert!(guard.dirty().load(Ordering::Relaxed));
        guard.iter().all(|&x| x == 0);
        guard[0] = 1;
        assert_eq!(guard[0], 1);
        assert!(guard.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_concurrent_read_access() {
        let (mut meta, mut page) = make_meta_and_page(0);
        let guard1 = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        let guard2 = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        assert_eq!(guard1.page_key(), &None);
        assert_eq!(guard2.page_key(), &None);
        assert!(!guard1.dirty().load(Ordering::Relaxed));
        assert!(!guard2.dirty().load(Ordering::Relaxed));
        guard1.iter().all(|&x| x == 0);
        guard2.iter().all(|&x| x == 0);
        assert!(!guard1.dirty().load(Ordering::Relaxed));
        assert!(!guard2.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_concurrent_write_access() {
        let (meta_box, page_box) = make_meta_and_page(0);

        struct MetaWrap {
            meta: UnsafeCell<Box<FrameMeta>>,
        }

        unsafe impl Send for MetaWrap {}
        unsafe impl Sync for MetaWrap {}

        impl MetaWrap {
            #[allow(clippy::mut_from_ref)]
            fn meta_mut(&self) -> &mut Box<FrameMeta> {
                unsafe { &mut *self.meta.get() }
            }
        }

        struct PageWrap {
            page: UnsafeCell<Box<Page>>,
        }

        unsafe impl Send for PageWrap {}
        unsafe impl Sync for PageWrap {}

        impl PageWrap {
            #[allow(clippy::mut_from_ref)]
            fn page_mut(&self) -> &mut Box<Page> {
                unsafe { &mut *self.page.get() }
            }
        }

        let meta_wrap = MetaWrap {
            meta: UnsafeCell::new(meta_box),
        };
        let page_wrap = PageWrap {
            page: UnsafeCell::new(page_box),
        };

        thread::scope(|scope| {
            for _ in 0..3 {
                let meta_wrap = &meta_wrap;
                let page_wrap = &page_wrap;
                scope.spawn(move || {
                    let meta_ptr = box_as_mut_ptr(meta_wrap.meta_mut());
                    let page_ptr = box_as_mut_ptr(page_wrap.page_mut());
                    for _ in 0..80 {
                        let mut guard = FrameWriteGuard::new(meta_ptr, page_ptr, true);
                        guard[0] += 1;
                    }
                });
            }
        }); // threads joined here

        let meta_ptr = box_as_mut_ptr(meta_wrap.meta_mut());
        let page_ptr = box_as_mut_ptr(page_wrap.page_mut());
        let guard = FrameReadGuard::new(meta_ptr, page_ptr);
        assert_eq!(guard[0], 240);
    }

    #[test]
    fn test_upgrade_access() {
        let (mut meta, mut page) = make_meta_and_page(0);
        {
            // Upgrade read guard to write guard and modify the first element
            let guard = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
            let mut guard = guard.try_upgrade(true).unwrap();
            assert_eq!(guard.page_key(), &None);
            assert!(guard.dirty().load(Ordering::Relaxed));
            guard.iter().all(|&x| x == 0);
            guard[0] = 1;
            assert_eq!(guard[0], 1);
            assert!(guard.dirty().load(Ordering::Relaxed));
        }
        let guard = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        assert_eq!(guard[0], 1);
        assert!(guard.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_downgrade_access() {
        let (mut meta, mut page) = make_meta_and_page(0);
        let mut guard =
            FrameWriteGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page), true);
        guard[0] = 1;
        let guard = guard.downgrade();
        assert_eq!(guard[0], 1);
        assert!(guard.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_upgrade_and_downgrade_access() {
        let (mut meta, mut page) = make_meta_and_page(0);
        // read -> write(dirty=false) -> read -> write(dirty=true) -> read
        let guard = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        assert!(!guard.dirty().load(Ordering::Relaxed));
        let mut guard = guard.try_upgrade(false).unwrap();
        guard[0] = 1;
        assert!(!guard.dirty().load(Ordering::Relaxed));
        let guard = guard.downgrade();
        assert!(!guard.dirty().load(Ordering::Relaxed));
        let mut guard = guard.try_upgrade(true).unwrap();
        guard[0] += 1;
        assert!(guard.dirty().load(Ordering::Relaxed));
        let guard = guard.downgrade();
        assert_eq!(guard[0], 2);
        assert!(guard.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_concurrent_upgrade_failure() {
        let (mut meta, mut page) = make_meta_and_page(0);
        let guard1 = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        let guard2 = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        match guard1.try_upgrade(true) {
            Ok(_) => panic!("Expected upgrade to fail"),
            Err(guard1) => {
                // Still holding the read guard of guard1
                assert!(guard2.try_upgrade(true).is_err());
                drop(guard1);
            }
        }
    }
}
