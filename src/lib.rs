/// Uses [`HashMap`] to store key-value pairs in memory.
use std::collections::HashMap;

/// A container for storing key-value pairs in memory.
pub struct KvStore {
    store: HashMap<String, String>,
}

impl Default for KvStore {
    fn default() -> Self {
        KvStore::new()
    }
}

/// Implementation of [`KvStore`]
impl KvStore {
    /// Constructs a [`KvStore`]
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::KvStore;
    ///
    /// let mut store = KvStore::new();
    /// ```
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// Sets a value corresponding to a key in the [`KvStore`]
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::KvStore;
    ///
    /// let mut store = KvStore::new();
    /// store.set(String::from("key1"), String::from("value1"));
    /// ```
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    /// Gets a value for a key from the [`KvStore`]
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::KvStore;
    ///
    /// let mut store = KvStore::new();
    /// store.set(String::from("key1"), String::from("value1"));
    /// store.get(String::from("key1"));
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        self.store.get(&key).cloned()
    }

    /// Removes a key from the [`KvStore`]
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::KvStore;
    ///
    /// let mut store = KvStore::new();
    /// store.set(String::from("key1"), String::from("value1"));
    /// store.remove(String::from("key1"));
    /// ```
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key);
    }
}
