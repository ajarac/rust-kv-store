use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
pub struct Versioned {
    ver: u64,
    tomb: bool,
    val: Vec<u8>,
}

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
        let versioned = Versioned { ver: next_version, tomb: false, val: value.to_vec() };
        self.map.insert(key.to_vec(), versioned).map(|old| old.val)
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map
            .get(key)
            .and_then(|x| if x.tomb { None } else { Some(x.val.clone()) })
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
}
