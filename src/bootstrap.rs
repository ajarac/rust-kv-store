use crate::types::key_value::KeyValue;
use crate::storage::Storage;
use std::io::Error;

pub fn rebuild_from_file(kv: &KeyValue, st: &Storage) -> Result<(), Error> {
    for rec in st.scan_all()? {
        let (k, value_option) = rec.unwrap();
        match value_option {
            Some(v) => { kv.put(k.as_slice(),  v.as_slice() ); }
            None    => { kv.delete(k.as_slice()); }
        }
    }
    Ok(())
}