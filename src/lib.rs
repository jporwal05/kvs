use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufReader, Seek, Write},
    path::PathBuf,
    result,
};

use chrono::Utc;
use failure::Error;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

/// Trigger compaction after number of stale records
const COMPACTION_TRIGGER: u32 = 500;

/// Default name for the log file
const STORE_NAME: &str = "kvs.store";

/// A [`Result`] that returns type `T` otherwise [`Error`]
pub type Result<T> = result::Result<T, Error>;

/// A container for storing key-value pairs in memory.
pub struct KvStore {
    index: HashMap<String, u64>,
    log: File,
    offsets_to_rm: HashSet<u64>,
    path: PathBuf,
}

/// Implementation of [`KvStore`]
impl KvStore {
    /// Opens a [`KvStore`] backed by a WAL at specified path
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::KvStore;
    /// # use tempfile::TempDir;
    ///
    /// let mut store = KvStore::open(TempDir::new().unwrap().path()).unwrap();
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut path_buf = PathBuf::from(path.into());
        path_buf.push(STORE_NAME);

        let file = open_file(&path_buf).unwrap();

        // replay log and create index
        let index = replay(&file)?;

        Ok(KvStore {
            log: file,
            index: index,
            offsets_to_rm: HashSet::new(),
            path: path_buf.parent().unwrap().to_path_buf(),
        })
    }

    /// Sets a value corresponding to a key in the [`KvStore`]
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::KvStore;
    /// # use tempfile::TempDir;
    ///
    /// let mut store = KvStore::open(TempDir::new().unwrap().path()).unwrap();
    /// store.set(String::from("key1"), String::from("value1"));
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command {
            key: key.to_string(),
            value: Some(value.to_string()),
            command_type: CommandType::SET,
        };
        let command_json = serde_json::to_string(&command).unwrap();
        let current_offset = self.log.seek(std::io::SeekFrom::End(0))?;
        self.log.write_all(command_json.as_bytes())?;
        // store the byte offset in the offsets_to_rm set if the key was overwritten
        self.index
            .insert(key.to_string(), current_offset)
            .map(|o| self.offsets_to_rm.insert(o));

        if self.offsets_to_rm.len() > COMPACTION_TRIGGER as usize {
            compact_log(self)?;
        }
        self.log.seek(std::io::SeekFrom::Start(0))?;
        Ok(())
    }

    /// Gets a value for a key from the [`KvStore`]
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::KvStore;
    /// # use tempfile::TempDir;
    ///
    /// let mut store = KvStore::open(TempDir::new().unwrap().path()).unwrap();
    /// store.set(String::from("key1"), String::from("value1"));
    /// store.get(String::from("key1"));
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let mut value: Option<String> = None;
        let mut found = false;
        if self.index.contains_key(&key) {
            self.log.seek(std::io::SeekFrom::Start(
                self.index.get(&key).unwrap().clone(),
            ))?;
            let mut stream = Deserializer::from_reader(BufReader::new(&self.log)) // new line
                .into_iter::<Command>();
            if let Some(Ok(c)) = stream.next() {
                value = c.value;
                found = true;
            }
        }

        if found {
            self.log.seek(std::io::SeekFrom::Start(0))?;
            println!("{}", value.as_ref().unwrap());
            return Ok(value);
        }
        Ok(None)
    }

    /// Removes a key from the [`KvStore`]
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::KvStore;
    /// # use tempfile::TempDir;
    ///
    /// let mut store = KvStore::open(TempDir::new().unwrap().path()).unwrap();
    /// store.set(String::from("key1"), String::from("value1"));
    /// store.remove(String::from("key1"));
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            self.index.remove(&key);
            let command = Command {
                key: key.to_string(),
                value: None,
                command_type: CommandType::RM,
            };
            let command_json = serde_json::to_string(&command)?;
            let bytes_offset = self.log.seek(std::io::SeekFrom::Current(0))?;
            self.offsets_to_rm.insert(bytes_offset);
            self.log.write_all(command_json.as_bytes())?;
            self.log.seek(std::io::SeekFrom::Start(0))?;
            Ok(())
        } else {
            Err(failure::err_msg("Key not found"))
        }
    }
}

/// Opens a file at a sepcified path. It creates the file it it doesn't already exist.
fn open_file(path: &PathBuf) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .read(true)
        .open(path)
        .map_err(|e| e.into())
}

/// Replay the log to create the index in-memory. This only keeps the valid keys in the index.
/// The index stores the key and the byte offset of the data stored in the log. If the log has a set entry for a key and then a remove entry then the key will effectively be removed from the index.
fn replay(file: &File) -> Result<HashMap<String, u64>> {
    let mut stream = Deserializer::from_reader(BufReader::new(file)) // new line
        .into_iter::<Command>();
    let mut index = HashMap::new();
    let mut byte_offset = 0;
    while let Some(Ok(c)) = stream.next() {
        if c.command_type == CommandType::RM {
            index.remove(&c.key);
        } else {
            index.insert(c.key.to_string(), byte_offset as u64);
        }
        byte_offset = stream.byte_offset();
    }
    Ok(index)
}

/// Compacts the log by replaying the log and recreating the index with effectively valid keys only.
/// It rebuilds the log as a new file and then renames it to the actual name.
fn compact_log(store: &mut KvStore) -> Result<()> {
    store.log.seek(std::io::SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(BufReader::new(&store.log)) // new line
        .into_iter::<Command>();
    let mut byte_offset = 0;
    let mut new_byte_offset = 0;
    let mut new_path = store.path.clone();
    new_path.push(format!("{}.{}", STORE_NAME, Utc::now()));
    // open a new file where the log will be rebuilt
    let mut new_log = open_file(&new_path).unwrap();
    // replay the current log
    while let Some(Ok(c)) = stream.next() {
        // skip the records to be removed
        if store.offsets_to_rm.contains(&byte_offset) {
            store.offsets_to_rm.remove(&byte_offset);
            byte_offset = stream.byte_offset() as u64;
            continue;
        }
        let bytes_written = new_log
            .write(serde_json::to_string(&c).unwrap().as_bytes())
            .unwrap();
        // insert valid records with new byte offset
        store.index.insert(c.key, new_byte_offset);
        new_byte_offset += bytes_written as u64;
        byte_offset = stream.byte_offset() as u64;
    }
    let mut old_path = store.path.clone();
    old_path.push(STORE_NAME);
    // rename the new log to the actual name
    fs::rename(&new_path, &old_path).unwrap();
    let mut new_path = store.path.clone();
    new_path.push(STORE_NAME);
    // point the log to the newly built, compacted log
    store.log = open_file(&new_path).unwrap();
    Ok(())
}

/// A container for storing commands
#[derive(Debug, Serialize, Deserialize)]
struct Command {
    key: String,
    value: Option<String>,
    command_type: CommandType,
}

/// Command type to identify the commands
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
enum CommandType {
    SET,
    GET,
    RM,
}
