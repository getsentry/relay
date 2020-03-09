use lazycell::AtomicLazyCell;

#[derive(Debug, Clone)]
pub struct SerdeFriendlyLazyCell<T>(AtomicLazyCell<T>);

impl<T> PartialEq for SerdeFriendlyLazyCell<T> {
    fn eq(&self, _other: &Self) -> bool {
        // for its uses in `PiiConfig` and `DataScrubbingConfig` it doesn't matter what is inside
        // this cell.
        true
    }
}

impl<T> Default for SerdeFriendlyLazyCell<T> {
    fn default() -> Self {
        SerdeFriendlyLazyCell::new()
    }
}

impl<T> SerdeFriendlyLazyCell<T> {
    pub fn new() -> Self {
        SerdeFriendlyLazyCell(AtomicLazyCell::new())
    }

    pub fn get_or_insert_with(&self, f: impl FnOnce() -> T) -> &T {
        if let Some(ref value) = self.0.borrow() {
            return value;
        }

        let value = f();
        self.0.fill(value).ok();

        // If filling the lazy cell fails, another thread is currently inserting. There are two
        // possible states:
        //  1. The cell is now filled. If we borrow the value now, we will get a response.
        //  2. The cell is locked but not filled. If we borrow the value now, we will get `None` and
        //     have to try again. Practically, this loop only executes once or twice.
        loop {
            match self.0.borrow() {
                Some(ref value) => break value,
                None => std::thread::sleep(std::time::Duration::from_micros(1)),
            }
        }
    }
}
