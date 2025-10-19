use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::types::versioned::Versioned;

pub struct KeyValue {
    map: DashMap<Vec<u8>, Versioned>,
    version: AtomicU64,
}

impl KeyValue {
    pub fn new() -> Self {
        KeyValue {
            map: DashMap::new(),
            version: AtomicU64::new(0),
        }
    }

    fn next_version(&self) -> u64 {
        self.version.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
        let next_version = self.next_version();
        let versioned = Versioned::from_value(next_version, value);
        self.map.insert(key.to_vec(), versioned).map(|old| old.val)
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map
            .get(key)
            .and_then(|x| if x.tomb { None } else { Some(x.val.clone()) })
    }

    pub fn delete(&self, key: &[u8]) -> Option<Vec<u8>> {
        let next_version = self.next_version();
        let versioned = Versioned::tomb(next_version);
        self.map
            .insert(key.to_vec(), versioned)
            .and_then(|old| if old.tomb { None } else { Some(old.val) })
    }
}

#[cfg(test)]
mod tests {
    use super::KeyValue;

    #[test]
    fn key_value_new() {
        let kv = KeyValue::new();
        assert!(kv.get(b"nonexistent").is_none());
    }

    #[test]
    fn key_value_put() {
        let kv = KeyValue::new();
        kv.put(b"key1", b"value1");
        assert_eq!(kv.get(b"key1"), Some(b"value1".to_vec()));
    }

    #[test]
    fn test_put_get() {
        let kv = KeyValue::new();
        kv.put(b"key1", b"value1");
        assert_eq!(kv.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(kv.get(b"key2"), None);
    }

    #[test]
    fn test_delete() {
        let kv = KeyValue::new();
        kv.delete(b"key1");
        assert_eq!(kv.get(b"key1"), None);
    }

    #[test]
    fn test_put_and_delete() {
        let kv = KeyValue::new();
        kv.put(b"key1", b"value1");
        assert_eq!(kv.get(b"key1"), Some(b"value1".to_vec()));
        kv.delete(b"key1");
        assert_eq!(kv.get(b"key1"), None);
    }
}
