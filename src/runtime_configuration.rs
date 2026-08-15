use arc_swap::ArcSwap;
use std::sync::Arc;

#[derive(Clone)]
pub struct RetainReadTimeValue<T> {
    inner: Arc<ArcSwap<T>>,
}

impl<T> RetainReadTimeValue<T> {
    pub fn new(initial: T) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    /// Get current value. This method gurantees that the pointer to read value always point to the
    /// read time value when accessed and not to the current one held by this struct
    #[must_use]
    pub fn get(&self) -> Arc<T> {
        self.inner.load_full()
    }

    /// Update value held by this struct. This method gurantees that existing readers which read
    /// the value earlier will not update it to the new one.
    pub fn update(&self, new_value: T) {
        self.inner.store(new_value.into());
    }
}
