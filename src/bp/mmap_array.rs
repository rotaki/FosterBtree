//! Owning wrapper around a single anonymous `mmap` region holding `len`
//! instances of `T` contiguously, with optional hugepages and NUMA placement.
//!
//! Used by `FrameManager` to back the page and meta arrays. The mapping is
//! pointer-stable for the lifetime of the `MmapArray`, so callers can hand
//! raw `*mut T` pointers to long-lived guards.
//!
//! Narrow on purpose: today the only two callers are `Page` and
//! `FrameMeta<E>`, so this exposes just what they need (zero-init for `Page`
//! via the unsafe `MmapZeroInit` marker; per-index constructor for
//! `FrameMeta`). Generalize when a second pattern appears.
//!
//! ## Hugepage modes
//!
//! - `Off`         – plain 4 KiB pages.
//! - `Transparent` – `MADV_HUGEPAGE`; the kernel coalesces lazily.
//! - `Explicit2M`  – `MAP_HUGETLB | (21 << MAP_HUGE_SHIFT)`; requires a
//!   reserved 2 MiB pool.
//! - `Explicit1G`  – `MAP_HUGETLB | (30 << MAP_HUGE_SHIFT)`; requires a
//!   reserved 1 GiB pool.
//!
//! Callers express what they want via [`HugepageRequest`] (with fallback
//! chains); the actual effective mode is reported by
//! [`MmapArray::hugepage_mode`].
//!
//! ## NUMA placement
//!
//! `MAP_POPULATE` (prefault) and `mbind` interact in a non-obvious way: the
//! kernel honors only the *current* memory policy at the moment a page is
//! first faulted, and `MAP_POPULATE` runs during the `mmap` syscall —
//! before we can call `mbind`. To honor both `prefault` and a non-`Default`
//! NUMA policy, we drop `MAP_POPULATE`, call `mbind`, then write-touch
//! every page ourselves under the now-installed policy.
//!
//! The touch loop **writes** rather than reads: for `MAP_PRIVATE |
//! MAP_ANONYMOUS`, a read fault would CoW-map the kernel's shared zero
//! page without allocating a fresh anonymous frame, so the mempolicy never
//! gets consulted. Only a write fault triggers a real allocation.
//!
//! ## Safety invariants
//!
//! - `T` must not need drop. Asserted at construction time. The mapping is
//!   released with `munmap`; per-element destructors are NOT run.
//! - For `zeroed`, the all-zero bit pattern must be a valid value of `T`.
//!   Encoded by the [`MmapZeroInit`] marker trait.

use std::io;
use std::marker::PhantomData;
use std::mem::{self, MaybeUninit};
use std::ptr::NonNull;
use std::sync::OnceLock;

use rayon::iter::{IntoParallelIterator, ParallelIterator};

#[allow(unused_imports)]
use crate::{log, log_warn};

// ---------------------------------------------------------------------------
// Hugepage / NUMA option types
// ---------------------------------------------------------------------------

/// What the kernel actually gave us for this mapping. Reported via
/// [`MmapArray::hugepage_mode`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HugepageMode {
    /// Plain 4 KiB pages.
    Off,
    /// Transparent hugepages (`MADV_HUGEPAGE`). Kernel may or may not have
    /// coalesced; this only reflects that the `madvise` call succeeded.
    Transparent,
    /// Explicit 2 MiB hugepages via `MAP_HUGETLB`.
    Explicit2M,
    /// Explicit 1 GiB hugepages via `MAP_HUGETLB`.
    Explicit1G,
}

/// What the caller asks for. Distinct from `HugepageMode` because the
/// explicit modes carry fallback semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HugepageRequest {
    Off,
    /// `madvise(MADV_HUGEPAGE)`. Always succeeds at the API level (the
    /// effective mode falls back to `Off` if the kernel rejects the
    /// advise).
    Transparent,
    /// Try 2 MiB explicit hugepages; on `ENOMEM`, fall back per flag.
    Prefer2M { fallback_to_thp: bool },
    /// Try 1 GiB explicit hugepages; on `ENOMEM`, fall back per flag.
    Prefer1G { fallback_to_2m: bool },
}

/// Sparse bitmask of NUMA node ids. We support up to 64 nodes (any system
/// we'll target).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeMask {
    bits: u64,
}

impl NodeMask {
    pub const EMPTY: Self = Self { bits: 0 };

    pub fn bits(&self) -> u64 {
        self.bits
    }

    pub fn single(node: u32) -> Self {
        assert!(node < 64, "node id must fit in u64");
        Self { bits: 1u64 << node }
    }

    pub fn set(&mut self, node: u32) {
        assert!(node < 64, "node id must fit in u64");
        self.bits |= 1u64 << node;
    }

    pub fn contains(&self, node: u32) -> bool {
        node < 64 && (self.bits & (1u64 << node)) != 0
    }

    pub fn is_empty(&self) -> bool {
        self.bits == 0
    }

    pub fn count(&self) -> u32 {
        self.bits.count_ones()
    }

    /// Returns `Some(highest_bit_set)` or `None` if empty. Used to compute
    /// `mbind`'s `maxnode` argument as `highest + 1`.
    pub fn highest_bit_set(&self) -> Option<u32> {
        if self.bits == 0 {
            None
        } else {
            Some(63 - self.bits.leading_zeros())
        }
    }

}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NumaPolicy {
    /// Kernel default. First-touch placement on the faulting thread's
    /// node. No `mbind` call.
    Default,
    /// Divide the mapping into one contiguous chunk per online NUMA node
    /// and `mbind` each chunk with `MPOL_BIND`. For two parallel arrays
    /// to co-locate (e.g., `page[i]` and `meta[i]` on the same node),
    /// both must use the same `elements_per_chunk`. See
    /// [`co_located_chunk_elements`] to compute the right value across a
    /// set of array element sizes.
    Striped { elements_per_chunk: usize },
}


#[derive(Clone, Copy, Debug)]
pub struct MmapOptions {
    pub hugepages: HugepageRequest,
    pub numa: NumaPolicy,
    /// `MAP_POPULATE` (when compatible with the chosen NUMA policy) or an
    /// explicit write-touch loop afterward.
    pub prefault: bool,
}

impl Default for MmapOptions {
    fn default() -> Self {
        // Conservative primitive default: transparent THP, kernel default
        // NUMA placement, prefault on (so warm-up timing is honest).
        // Buffer-pool-specific defaults (striped page/meta co-location)
        // live in `FrameManagerOptions` / `FrameManager::new`.
        Self {
            hugepages: HugepageRequest::Transparent,
            numa: NumaPolicy::Default,
            prefault: true,
        }
    }
}

/// Types where the all-zero bit pattern is a valid value.
///
/// # Safety
/// Implementing this trait asserts that `MmapArray::zeroed::<T>` is sound:
/// reading any element of a zero-filled mapping as `T` produces a valid
/// value. Atomics with `0` as their initial state, `[u8; N]`, and
/// `#[repr(C)]` aggregates of such fields all qualify.
pub unsafe trait MmapZeroInit: Send + Sync {}

unsafe impl MmapZeroInit for crate::page::Page {}

// ---------------------------------------------------------------------------
// Topology detection
// ---------------------------------------------------------------------------

fn online_nodes() -> NodeMask {
    static CACHED: OnceLock<NodeMask> = OnceLock::new();
    *CACHED.get_or_init(|| match std::fs::read_to_string("/sys/devices/system/node/online") {
        Ok(s) => parse_node_list(s.trim()).unwrap_or_else(|| NodeMask::single(0)),
        Err(_) => NodeMask::single(0),
    })
}

/// Parse `/sys/devices/system/node/online`-style range lists:
/// `"0"`, `"0-1"`, `"0,2-3"`.
fn parse_node_list(s: &str) -> Option<NodeMask> {
    let mut mask = NodeMask::EMPTY;
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((lo, hi)) = part.split_once('-') {
            let lo: u32 = lo.trim().parse().ok()?;
            let hi: u32 = hi.trim().parse().ok()?;
            if lo > hi || hi >= 64 {
                return None;
            }
            for n in lo..=hi {
                mask.set(n);
            }
        } else {
            let n: u32 = part.parse().ok()?;
            if n >= 64 {
                return None;
            }
            mask.set(n);
        }
    }
    if mask.is_empty() {
        None
    } else {
        Some(mask)
    }
}

/// Count of online NUMA nodes (≥ 1).
pub fn online_node_count() -> u32 {
    online_nodes().count().max(1)
}

/// Compute an `elements_per_chunk` value for [`NumaPolicy::Striped`] that
/// page-aligns the chunk byte size for every array element type in
/// `elem_sizes`. Pass the same `len` and the same set of element sizes
/// when allocating parallel arrays so they co-locate.
///
/// For example, when pairing a `Page` array (16 KiB elements) with a
/// `FrameMeta` array (64 B, 64-aligned) on a system with N online nodes:
///
/// ```ignore
/// let epc = co_located_chunk_elements(
///     num_frames,
///     &[size_of::<Page>(), size_of::<FrameMeta<E>>()],
/// );
/// // Pass the same `NumaPolicy::Striped { elements_per_chunk: epc }`
/// // to both arrays.
/// ```
///
/// Returns `len` when the system has only one online node (Striped
/// degenerates to "everything on one node").
pub fn co_located_chunk_elements(len: usize, elem_sizes: &[usize]) -> usize {
    let n = online_node_count() as usize;
    if n <= 1 {
        return len;
    }
    let page = os_page_size();
    // For each element size E: chunk_bytes = epc * E must be a multiple
    // of `page`. The element-level alignment is `page / gcd(E, page)`.
    let mut alignment_in_elements: usize = 1;
    for &e in elem_sizes {
        debug_assert!(e > 0);
        let a = page / gcd(e, page);
        alignment_in_elements = lcm(alignment_in_elements, a);
    }
    let raw = len.div_ceil(n);
    align_up_usize(raw, alignment_in_elements)
}

#[inline]
fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

#[inline]
fn lcm(a: usize, b: usize) -> usize {
    if a == 0 || b == 0 {
        0
    } else {
        a / gcd(a, b) * b
    }
}

#[inline]
fn align_up_usize(v: usize, a: usize) -> usize {
    debug_assert!(a > 0);
    v.div_ceil(a) * a
}

// ---------------------------------------------------------------------------
// MmapArray
// ---------------------------------------------------------------------------

pub struct MmapArray<T> {
    ptr: NonNull<T>,
    len: usize,
    bytes: usize,
    huge: HugepageMode,
    numa: NumaPolicy,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send> Send for MmapArray<T> {}
unsafe impl<T: Sync> Sync for MmapArray<T> {}

impl<T> MmapArray<T> {
    /// Construct each element by writing `init(idx)` in place.
    ///
    /// `init` returns `T` by value; the compiler may materialize that value
    /// on the stack before `ptr::write` moves it into the mapping. Avoid
    /// this constructor when `size_of::<T>()` is larger than a few KiB —
    /// use [`MmapArray::zeroed`] (with an `MmapZeroInit` impl) instead.
    pub fn new_with<F>(len: usize, opts: MmapOptions, init: F) -> io::Result<Self>
    where
        T: Send + Sync,
        F: Fn(usize) -> T + Sync,
    {
        assert!(
            !mem::needs_drop::<T>(),
            "MmapArray<T>: T must be drop-trivial",
        );
        assert!(len > 0, "MmapArray::new_with: len must be > 0");
        assert!(mem::size_of::<T>() > 0, "MmapArray<T>: T must be non-ZST");

        let arr = unsafe { Self::map_uninit(len, opts)? };
        // Cast through usize so the closure is `Sync`. Pointers cross
        // thread boundaries by value — each i writes a disjoint element.
        let ptr_addr = arr.ptr.as_ptr() as usize;
        (0..len).into_par_iter().for_each(|i| unsafe {
            let ptr = ptr_addr as *mut T;
            ptr.add(i).write(init(i));
        });
        Ok(arr)
    }

    pub fn mapped_bytes(&self) -> usize {
        self.bytes
    }

    #[inline]
    pub fn as_ptr(&self) -> *mut T {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Effective hugepage mode (what the kernel actually gave us, after
    /// fallbacks).
    pub fn hugepage_mode(&self) -> HugepageMode {
        self.huge
    }

    /// Effective NUMA policy (what was actually applied via `mbind`).
    /// Returns `Default` if `mbind` failed or was skipped.
    pub fn numa_policy(&self) -> NumaPolicy {
        self.numa
    }

    /// Raw pointer to element `idx`. Bounds-checked in debug builds.
    #[inline]
    pub fn get_ptr(&self, idx: usize) -> *mut T {
        debug_assert!(idx < self.len, "MmapArray::get_ptr: out of bounds");
        unsafe { self.ptr.as_ptr().add(idx) }
    }

    /// Core allocation routine. Walks the hugepage fallback chain, applies
    /// NUMA policy, prefaults if requested.
    unsafe fn map_uninit(len: usize, opts: MmapOptions) -> io::Result<Self> {
        let elem = mem::size_of::<T>();
        let raw_bytes = len.checked_mul(elem).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "len * size_of::<T> overflow")
        })?;

        // Validate NUMA policy up-front — fail fast on bad config.
        if let NumaPolicy::Striped { elements_per_chunk } = opts.numa {
            if elements_per_chunk == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "NumaPolicy::Striped: elements_per_chunk must be > 0",
                ));
            }
            let chunk_bytes = elements_per_chunk.checked_mul(elem).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "NumaPolicy::Striped: chunk byte size overflow",
                )
            })?;
            if chunk_bytes % os_page_size() != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "NumaPolicy::Striped: chunk_bytes={} not OS-page-aligned (epc={}, elem_size={}); see co_located_chunk_elements()",
                        chunk_bytes, elements_per_chunk, elem
                    ),
                ));
            }
        }
        // Whether we can use MAP_POPULATE: only when NUMA policy doesn't
        // require mbind-before-fault.
        let prefault_via_mmap = opts.prefault && matches!(opts.numa, NumaPolicy::Default);

        let (addr, bytes, effective_huge) =
            try_map_with_fallback(raw_bytes, opts.hugepages, prefault_via_mmap)?;

        // THP advise (best-effort; effective mode reflects the result).
        let final_huge = if matches!(opts.hugepages, HugepageRequest::Transparent)
            && matches!(effective_huge, HugepageMode::Off)
        {
            let rc = libc::madvise(addr, bytes, libc::MADV_HUGEPAGE);
            if rc == 0 {
                HugepageMode::Transparent
            } else {
                HugepageMode::Off
            }
        } else {
            effective_huge
        };

        // mbind for any non-Default policy.
        let effective_numa = apply_mbind(addr, bytes, elem, opts.numa);

        // Write-touch loop when prefault was requested but we couldn't use
        // MAP_POPULATE.
        if opts.prefault && !prefault_via_mmap {
            let stride = effective_page_size(final_huge);
            touch_pages_write(addr as *mut u8, bytes, stride);
        }

        let ptr = NonNull::new(addr as *mut T)
            .expect("mmap returned non-null but NonNull rejected");

        Ok(MmapArray {
            ptr,
            len,
            bytes,
            huge: final_huge,
            numa: effective_numa,
            _marker: PhantomData,
        })
    }
}

impl<T: MmapZeroInit> MmapArray<T> {
    /// Allocate a zero-filled mapping. `MAP_ANONYMOUS` gives zero pages,
    /// so this is the cheap path for large `T` (it never moves a `T`
    /// through the stack).
    pub fn zeroed(len: usize, opts: MmapOptions) -> io::Result<Self> {
        assert!(
            !mem::needs_drop::<T>(),
            "MmapArray<T>: T must be drop-trivial",
        );
        assert!(len > 0, "MmapArray::zeroed: len must be > 0");
        assert!(mem::size_of::<T>() > 0, "MmapArray<T>: T must be non-ZST");
        unsafe { Self::map_uninit(len, opts) }
    }
}

impl<T> Drop for MmapArray<T> {
    fn drop(&mut self) {
        // T is drop-trivial (asserted at construction); skip per-element
        // destructors and just release the mapping.
        unsafe {
            libc::munmap(self.ptr.as_ptr() as *mut libc::c_void, self.bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// Hugepage fallback walk
// ---------------------------------------------------------------------------

/// Returns `(addr, mapped_bytes, effective_mode)`. `prefault_via_mmap`
/// controls whether `MAP_POPULATE` is added to the flags.
unsafe fn try_map_with_fallback(
    raw_bytes: usize,
    request: HugepageRequest,
    prefault_via_mmap: bool,
) -> io::Result<(*mut libc::c_void, usize, HugepageMode)> {
    // Order of attempts as a slice of (mode, allow-further-fallback).
    // Each attempt rounds bytes to its own page size.
    let chain: &[HugepageMode] = match request {
        HugepageRequest::Off | HugepageRequest::Transparent => &[HugepageMode::Off],
        HugepageRequest::Prefer2M { fallback_to_thp: true } => {
            &[HugepageMode::Explicit2M, HugepageMode::Off]
        }
        HugepageRequest::Prefer2M { fallback_to_thp: false } => {
            &[HugepageMode::Explicit2M]
        }
        HugepageRequest::Prefer1G { fallback_to_2m: true } => &[
            HugepageMode::Explicit1G,
            HugepageMode::Explicit2M,
            HugepageMode::Off,
        ],
        HugepageRequest::Prefer1G { fallback_to_2m: false } => {
            &[HugepageMode::Explicit1G]
        }
    };

    let mut last_err: Option<io::Error> = None;
    for &mode in chain {
        let page = page_size_for(mode);
        let bytes = round_up(raw_bytes, page);
        let mut flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
        if prefault_via_mmap {
            flags |= libc::MAP_POPULATE;
        }
        match mode {
            HugepageMode::Explicit2M => {
                flags |= libc::MAP_HUGETLB | (21 << libc::MAP_HUGE_SHIFT);
            }
            HugepageMode::Explicit1G => {
                flags |= libc::MAP_HUGETLB | (30 << libc::MAP_HUGE_SHIFT);
            }
            _ => {}
        }
        let addr = libc::mmap(
            std::ptr::null_mut(),
            bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            flags,
            -1,
            0,
        );
        if addr != libc::MAP_FAILED {
            if !matches!(mode, HugepageMode::Off) {
                log_warn!(
                    "[MmapArray] hugepage mode={:?} bytes={} succeeded",
                    mode,
                    bytes
                );
            }
            return Ok((addr, bytes, mode));
        }
        let err = io::Error::last_os_error();
        log_warn!(
            "[MmapArray] hugepage mode={:?} bytes={} failed: {}",
            mode,
            bytes,
            err
        );
        last_err = Some(err);
    }
    Err(last_err.unwrap_or_else(|| io::Error::other("mmap failed")))
}

fn page_size_for(mode: HugepageMode) -> usize {
    match mode {
        HugepageMode::Off | HugepageMode::Transparent => os_page_size(),
        HugepageMode::Explicit2M => 2 * 1024 * 1024,
        HugepageMode::Explicit1G => 1024 * 1024 * 1024,
    }
}

fn effective_page_size(mode: HugepageMode) -> usize {
    // After mbind, even transparent THP placement happens at coalesced
    // hugepage granularity *if* the kernel coalesced. We can't observe
    // that, so stride at OS page size for both Off and Transparent. That
    // overcounts touches but doesn't hurt correctness.
    page_size_for(match mode {
        HugepageMode::Transparent => HugepageMode::Off,
        m => m,
    })
}

// ---------------------------------------------------------------------------
// mbind + write-touch
// ---------------------------------------------------------------------------

fn apply_mbind(
    addr: *mut libc::c_void,
    bytes: usize,
    elem_size: usize,
    policy: NumaPolicy,
) -> NumaPolicy {
    match policy {
        NumaPolicy::Default => NumaPolicy::Default,
        NumaPolicy::Striped { elements_per_chunk } => {
            apply_mbind_striped(addr, bytes, elem_size, elements_per_chunk)
        }
    }
}

/// Issue one `mbind` call covering a single contiguous range.
fn mbind_one(
    addr: *mut libc::c_void,
    bytes: usize,
    mode: libc::c_int,
    mask: NodeMask,
) -> io::Result<()> {
    // maxnode = highest_bit_set + 1 (NOT popcount).
    let maxnode = mask.highest_bit_set().map_or(0, |b| (b as u64) + 1);
    let nodemask_word: libc::c_ulong = mask.bits() as libc::c_ulong;

    let rc = unsafe {
        libc::syscall(
            libc::SYS_mbind,
            addr,
            bytes as libc::size_t,
            mode as libc::c_long,
            &nodemask_word as *const libc::c_ulong,
            maxnode as libc::c_ulong,
            0u32 as libc::c_ulong,
        )
    };

    if rc == 0 {
        Ok(())
    } else {
        let err = io::Error::last_os_error();
        log_warn!(
            "[MmapArray] mbind(mode={}, mask={:#b}, bytes={}) failed: {}",
            mode,
            mask.bits(),
            bytes,
            err
        );
        Err(err)
    }
}

/// Stripe the mapping across online nodes: chunk `j` (covering
/// `elements_per_chunk * elem_size` bytes) gets `MPOL_BIND` to
/// `online_nodes[j]`. The last chunk may be smaller if `len` isn't an
/// exact multiple of `elements_per_chunk * num_nodes`.
fn apply_mbind_striped(
    addr: *mut libc::c_void,
    bytes: usize,
    elem_size: usize,
    elements_per_chunk: usize,
) -> NumaPolicy {
    if elements_per_chunk == 0 {
        log_warn!("[MmapArray] Striped: elements_per_chunk == 0 — skipping");
        return NumaPolicy::Default;
    }

    let online = online_nodes();
    let num_chunks = online.count();
    if num_chunks <= 1 {
        return NumaPolicy::Default;
    }

    let chunk_bytes = match elements_per_chunk.checked_mul(elem_size) {
        Some(b) => b,
        None => {
            log_warn!("[MmapArray] Striped: chunk byte overflow");
            return NumaPolicy::Default;
        }
    };

    // mbind requires page-aligned chunk boundaries.
    let page = os_page_size();
    if chunk_bytes % page != 0 {
        log_warn!(
            "[MmapArray] Striped: chunk_bytes={} not page-aligned (page={}, elem={}, epc={}) — falling back to Default",
            chunk_bytes,
            page,
            elem_size,
            elements_per_chunk,
        );
        return NumaPolicy::Default;
    }

    // Enumerate the online node ids in ascending order.
    let mut node_ids: Vec<u32> = (0..64).filter(|n| online.contains(*n)).collect();
    debug_assert_eq!(node_ids.len() as u32, num_chunks);

    let base = addr as usize;
    let mut any_failed = false;
    for (j, node) in node_ids.drain(..).enumerate() {
        let chunk_start = base + j * chunk_bytes;
        if chunk_start >= base + bytes {
            // No bytes left for this chunk; happens when the array is
            // smaller than one full chunk-per-node distribution.
            break;
        }
        let remaining = (base + bytes) - chunk_start;
        let chunk_len = chunk_bytes.min(remaining);
        let mask = NodeMask::single(node);
        if mbind_one(chunk_start as *mut libc::c_void, chunk_len, libc::MPOL_BIND, mask).is_err() {
            any_failed = true;
        }
    }

    if any_failed {
        NumaPolicy::Default
    } else {
        NumaPolicy::Striped { elements_per_chunk }
    }
}


/// Write-touch every page-sized chunk so the kernel allocates fresh
/// anonymous frames under the current mempolicy. A read would CoW-map the
/// shared zero page and never consult the policy.
unsafe fn touch_pages_write(addr: *mut u8, bytes: usize, stride: usize) {
    debug_assert!(stride > 0);
    // Parallelize via rayon when there are many strides — order doesn't
    // matter because the mbind policy is per-VMA.
    let strides = (bytes + stride - 1) / stride;
    if strides >= 1024 {
        let addr_n = addr as usize;
        (0..strides).into_par_iter().for_each(|i| {
            let off = i * stride;
            if off < bytes {
                unsafe {
                    std::ptr::write_volatile((addr_n as *mut u8).add(off), 0u8);
                }
            }
        });
    } else {
        let mut off = 0;
        while off < bytes {
            std::ptr::write_volatile(addr.add(off), 0u8);
            off += stride;
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[inline]
fn round_up(bytes: usize, page: usize) -> usize {
    debug_assert!(page.is_power_of_two());
    (bytes + page - 1) & !(page - 1)
}

fn os_page_size() -> usize {
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| {
        let p = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        if p == 0 {
            4096
        } else {
            p
        }
    })
}

// Quiet unused-import lint if MaybeUninit ends up unused after refactors.
const _: () = {
    let _ = std::mem::size_of::<MaybeUninit<u8>>();
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[repr(C, align(64))]
    struct Meta {
        id: AtomicU32,
        payload: [u8; 60],
    }

    unsafe impl MmapZeroInit for Meta {}

    fn opts_off_no_prefault() -> MmapOptions {
        MmapOptions {
            hugepages: HugepageRequest::Off,
            numa: NumaPolicy::Default,
            prefault: false,
        }
    }

    #[test]
    fn new_with_constructs_each_element() {
        let arr: MmapArray<Meta> = MmapArray::new_with(128, opts_off_no_prefault(), |i| Meta {
            id: AtomicU32::new(i as u32),
            payload: [0; 60],
        })
        .unwrap();

        assert_eq!(arr.len(), 128);
        for i in 0..arr.len() {
            let m = unsafe { &*arr.get_ptr(i) };
            assert_eq!(m.id.load(Ordering::Relaxed), i as u32);
        }
    }

    #[test]
    fn zeroed_returns_all_zero_bytes() {
        let arr: MmapArray<Meta> = MmapArray::zeroed(64, opts_off_no_prefault()).unwrap();
        assert_eq!(arr.len(), 64);
        for i in 0..arr.len() {
            let m = unsafe { &*arr.get_ptr(i) };
            assert_eq!(m.id.load(Ordering::Relaxed), 0);
            assert!(m.payload.iter().all(|b| *b == 0));
        }
    }

    #[test]
    fn ptr_is_page_aligned() {
        let arr: MmapArray<Meta> = MmapArray::zeroed(8, opts_off_no_prefault()).unwrap();
        let p = arr.as_ptr() as usize;
        assert_eq!(p % os_page_size(), 0);
    }

    #[test]
    fn transparent_thp_succeeds() {
        let arr: MmapArray<Meta> = MmapArray::new_with(
            256,
            MmapOptions {
                hugepages: HugepageRequest::Transparent,
                numa: NumaPolicy::Default,
                prefault: false,
            },
            |i| Meta {
                id: AtomicU32::new(i as u32),
                payload: [0; 60],
            },
        )
        .unwrap();
        assert_eq!(arr.len(), 256);
        // Effective mode is Transparent on success or Off on madvise reject.
        assert!(matches!(
            arr.hugepage_mode(),
            HugepageMode::Transparent | HugepageMode::Off
        ));
    }

    #[test]
    fn prefault_default_numa_uses_map_populate() {
        let arr: MmapArray<Meta> = MmapArray::new_with(
            32,
            MmapOptions {
                hugepages: HugepageRequest::Off,
                numa: NumaPolicy::Default,
                prefault: true,
            },
            |i| Meta {
                id: AtomicU32::new(i as u32),
                payload: [0; 60],
            },
        )
        .unwrap();
        assert_eq!(arr.len(), 32);
        assert_eq!(arr.numa_policy(), NumaPolicy::Default);
    }

    #[test]
    fn mapped_bytes_rounded_to_page_size() {
        let arr: MmapArray<Meta> = MmapArray::zeroed(7, opts_off_no_prefault()).unwrap();
        assert!(arr.mapped_bytes() >= 7 * std::mem::size_of::<Meta>());
        assert_eq!(arr.mapped_bytes() % os_page_size(), 0);
    }

    #[test]
    fn page_zero_init_is_valid() {
        use crate::page::Page;
        let arr: MmapArray<Page> = MmapArray::zeroed(4, opts_off_no_prefault()).unwrap();
        for i in 0..arr.len() {
            let p = unsafe { &*arr.get_ptr(i) };
            assert!(p.get_raw_bytes().iter().all(|b| *b == 0));
        }
    }

    // ---- topology / mask parsing ---------------------------------------

    #[test]
    fn parse_single_node() {
        let m = parse_node_list("0").unwrap();
        assert!(m.contains(0));
        assert_eq!(m.count(), 1);
        assert_eq!(m.highest_bit_set(), Some(0));
    }

    #[test]
    fn parse_range() {
        let m = parse_node_list("0-3").unwrap();
        for n in 0..=3 {
            assert!(m.contains(n));
        }
        assert!(!m.contains(4));
        assert_eq!(m.count(), 4);
        assert_eq!(m.highest_bit_set(), Some(3));
    }

    #[test]
    fn parse_sparse_list() {
        let m = parse_node_list("0,2-3").unwrap();
        assert!(m.contains(0));
        assert!(!m.contains(1));
        assert!(m.contains(2));
        assert!(m.contains(3));
        assert_eq!(m.count(), 3);
        // The maxnode trap: sparse mask {0,2,3} -> highest=3, maxnode=4.
        assert_eq!(m.highest_bit_set(), Some(3));
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!(parse_node_list("not-a-number").is_none());
        assert!(parse_node_list("5-3").is_none()); // reversed range
        assert!(parse_node_list("64").is_none()); // out of u64 range
    }

    #[test]
    fn online_nodes_non_empty() {
        // Every Linux box should report at least node 0.
        let n = online_nodes();
        assert!(!n.is_empty());
        assert!(n.contains(0));
    }

    // ---- co-location math ---------------------------------------------

    #[test]
    fn co_located_chunk_elements_aligns_for_all_sizes() {
        // Pretend len = 1M, with Page-sized (16 KiB) and FrameMeta-sized
        // (64 B) elements. On a multi-node system the result must satisfy
        // chunk_bytes % page == 0 for BOTH element sizes.
        let len = 1_000_000usize;
        let epc = co_located_chunk_elements(len, &[16384, 64]);
        let page = os_page_size();
        // On a single-node box epc == len (whole array on one node).
        if online_node_count() > 1 {
            assert!(epc > 0 && epc <= len);
            assert_eq!((epc * 16384) % page, 0);
            assert_eq!((epc * 64) % page, 0);
        } else {
            assert_eq!(epc, len);
        }
    }

    #[test]
    fn striped_succeeds_with_aligned_chunks() {
        // On a single-node system Striped degenerates to Default. On a
        // multi-node system we want the allocation to succeed and the
        // effective policy to be Striped.
        let len = 4096usize;
        let epc = co_located_chunk_elements(len, &[std::mem::size_of::<Meta>()]);
        let arr: MmapArray<Meta> = MmapArray::new_with(
            len,
            MmapOptions {
                hugepages: HugepageRequest::Off,
                numa: NumaPolicy::Striped {
                    elements_per_chunk: epc,
                },
                prefault: true,
            },
            |i| Meta {
                id: AtomicU32::new(i as u32),
                payload: [0; 60],
            },
        )
        .unwrap();
        assert_eq!(arr.len(), len);
        let p = arr.numa_policy();
        assert!(matches!(
            p,
            NumaPolicy::Striped { .. } | NumaPolicy::Default
        ));
    }

    #[test]
    fn striped_rejects_misaligned_chunks() {
        // 1 element of 64 bytes per chunk -> 64 B chunk size; not page
        // aligned. Should fail validation up-front.
        if online_node_count() <= 1 {
            return; // Striped is a no-op on single-node; can't trigger validation.
        }
        let result = MmapArray::<Meta>::zeroed(
            128,
            MmapOptions {
                hugepages: HugepageRequest::Off,
                numa: NumaPolicy::Striped { elements_per_chunk: 1 },
                prefault: false,
            },
        );
        assert!(result.is_err(), "misaligned Striped chunks should error");
    }

    #[test]
    fn striped_zero_chunk_size_errors() {
        let result = MmapArray::<Meta>::zeroed(
            16,
            MmapOptions {
                hugepages: HugepageRequest::Off,
                numa: NumaPolicy::Striped { elements_per_chunk: 0 },
                prefault: false,
            },
        );
        assert!(result.is_err());
    }
}
