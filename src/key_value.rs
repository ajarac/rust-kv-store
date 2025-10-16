use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct KeyValue(Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>);

impl KeyValue {
    pub fn new() -> Self {
        KeyValue(Arc::new(RwLock::new(HashMap::new())))
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        let mut map = self.0.write().unwrap();
        map.insert(key.to_vec(), value.to_vec());
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let map = self.0.read().unwrap();
        map.get(key).cloned()
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