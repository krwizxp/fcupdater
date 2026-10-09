struct OptAllocator;
pub(crate) static OPT_ALLOC_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub(crate) static OPT_ALLOC_BYTES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
// SAFETY: Every operation delegates unchanged to the system allocator.
unsafe impl std::alloc::GlobalAlloc for OptAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        OPT_ALLOC_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        OPT_ALLOC_BYTES.fetch_add(layout.size(), std::sync::atomic::Ordering::Relaxed);
        // SAFETY: The caller supplies a valid allocation layout.
        unsafe { std::alloc::System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        // SAFETY: The caller supplies a live system allocation and its layout.
        unsafe { std::alloc::System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, size: usize) -> *mut u8 {
        OPT_ALLOC_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        OPT_ALLOC_BYTES.fetch_add(size, std::sync::atomic::Ordering::Relaxed);
        // SAFETY: The caller supplies a live system allocation and valid new size.
        unsafe { std::alloc::System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static OPT_ALLOCATOR: OptAllocator = OptAllocator;
