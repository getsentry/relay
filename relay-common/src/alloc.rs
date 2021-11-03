use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct RelayAllocator {
    max_size: AtomicUsize,
}

unsafe impl GlobalAlloc for RelayAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let max_size = self.max_size.load(Ordering::Relaxed);
        if max_size > 0 && layout.size() > max_size {
            panic!(
                "attempted to allocate {} bytes, crashing thread",
                layout.size()
            );
        }

        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: RelayAllocator = RelayAllocator {
    max_size: AtomicUsize::new(0),
};

/// Set a max allocation size, default is to have none.
pub fn set_max_alloc_size(max_size: Option<usize>) {
    let max_size = match max_size {
        Some(0) => panic!("max_size should probably be None"),
        Some(x) => x,
        None => 0,
    };

    GLOBAL.max_size.store(max_size, Ordering::Relaxed);
}
