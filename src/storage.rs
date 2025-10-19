use std::fs::{File, OpenOptions};
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct Storage {
    path: PathBuf,
}

impl Storage {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, Error> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).expect("Error creating storage directory");
        let path = dir.join("active.segment");
        // ensure file exists
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self { path })
    }

    pub fn append_put(&self, key: &[u8], val: &[u8]) -> Result<(), Error> {
        self.append_inner(key, Some(val))
    }
    pub fn append_delete(&self, key: &[u8]) -> Result<(), Error> {
        self.append_inner(key, None)
    }

    fn append_inner(&self, key: &[u8], val: Option<&[u8]>) -> Result<(), Error> {
        let mut f = OpenOptions::new().read(true).write(true).open(&self.path)?;
        f.seek(SeekFrom::End(0))?;
        let key_len = key.len() as u32;
        let v_bytes: &[u8] = val.unwrap_or(&[]);
        let value_len = v_bytes.len() as u32;
        f.write_all(&key_len.to_le_bytes())?;
        f.write_all(&value_len.to_le_bytes())?;
        f.write_all(key)?;
        if value_len > 0 {
            f.write_all(v_bytes)?;
        }
        f.flush()?; // simple durability; use fdatasync for stronger guarantees
        // f.sync_data()?; //
        Ok(())
    }

    pub fn scan_all(&self) -> Result<ScanIter, Error> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        Ok(ScanIter { file, done: false })
    }
}

pub struct ScanIter {
    file: File,
    done: bool,
}
impl Iterator for ScanIter {
    type Item = anyhow::Result<(Vec<u8>, Option<Vec<u8>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut lens = [0u8; 8];
        if let Err(e) = self.file.read_exact(&mut lens) {
            self.done = true;
            // EOF cleanly → None; other errors → surface once, then stop
            return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                None
            } else {
                Some(Err(e.into()))
            };
        }
        let key_len = u32::from_le_bytes(lens[0..4].try_into().unwrap()) as usize;
        let value_len = u32::from_le_bytes(lens[4..8].try_into().unwrap()) as usize;

        let mut key = vec![0u8; key_len];
        if let Err(e) = self.file.read_exact(&mut key) {
            self.done = true;
            return Some(Err(e.into()));
        }

        if value_len == 0 {
            return Some(Ok((key, None)));
        }
        let mut val = vec![0u8; value_len];
        if let Err(e) = self.file.read_exact(&mut val) {
            self.done = true;
            return Some(Err(e.into()));
        }
        Some(Ok((key, Some(val))))
    }
}
