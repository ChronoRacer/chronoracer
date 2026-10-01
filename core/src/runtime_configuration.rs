use arc_swap::ArcSwap;
use std::sync::Arc;

/// Holds a value that can be updated while existing readers keep their snapshot.
///
/// Intended to be `Box::leak`ed to `&'static` at startup.
pub struct RetainReadTimeValue<T> {
    inner: ArcSwap<T>,
}

impl<T> RetainReadTimeValue<T> {
    pub fn new(initial: T) -> Self {
        Self {
            inner: ArcSwap::from_pointee(initial),
        }
    }

    /// Get current value. The returned `Arc` keeps the value from this call; a later `update` does
    /// not change it.
    #[must_use]
    pub fn get(&self) -> Arc<T> {
        self.inner.load_full()
    }

    /// Replace the value. Readers that already called `get` keep their previous `Arc`.
    pub fn update(&self, new_value: T) {
        self.inner.store(new_value.into());
    }
}
