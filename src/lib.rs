use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufReader, Seek, Write},
    path::PathBuf,
    result,
};

use failure::Error;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

/// Trigger compaction after number of stale records
const COMPACTION_TRIGGER: u32 = 500;

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
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut path_buf = PathBuf::from(path.into());
        path_buf.push("kvs.store");

        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .read(true)
            .open(&path_buf)
            .unwrap();

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
        // TODO: log compaction strategy
        // get to know if key already existed or not and get the previous offset if it did
        // maintain a separate set of offsets to be removed from the file
        // seek from the beginning and read as stream
        // skip the offsets to be removed by doing a look-up from the set
        // write the valid entries in another file
        // make the store point to the new file and then continue
        // do same thing on every write but only if the set has grown to a certain amount - this is to avoid frequent compaction
        // check efficiency for removal of values from a set - or else try another data structure, stack?
        self.index
            .insert(key.to_string(), current_offset)
            .map(|o| self.offsets_to_rm.insert(o));

        if self.offsets_to_rm.len() > COMPACTION_TRIGGER as usize {
            self.log.seek(std::io::SeekFrom::Start(0))?;
            let mut stream = Deserializer::from_reader(BufReader::new(&self.log)) // new line
                .into_iter::<Command>();
            let mut byte_offset = 0;
            let mut new_byte_offset = 0;
            let mut new_path = self.path.clone();
            new_path.push("kvs_new.store");
            let mut new_log = OpenOptions::new()
                .create(true)
                .append(true)
                .write(true)
                .open(&new_path)
                .unwrap();
            while let Some(Ok(c)) = stream.next() {
                if self.offsets_to_rm.contains(&byte_offset) {
                    self.offsets_to_rm.remove(&byte_offset);
                    byte_offset = stream.byte_offset() as u64;
                    continue;
                }
                let bytes_written = new_log
                    .write(serde_json::to_string(&c).unwrap().as_bytes())
                    .unwrap();
                self.index.insert(c.key, new_byte_offset);
                new_byte_offset += bytes_written as u64;
                byte_offset = stream.byte_offset() as u64;
            }
            // change the log
            let mut old_path = self.path.clone();
            old_path.push("kvs.store");
            fs::rename(&new_path, &old_path).unwrap();
            let mut new_path = self.path.clone();
            new_path.push("kvs.store");
            self.log = OpenOptions::new()
                .append(true)
                .read(true)
                .write(true)
                .open(&new_path)
                .unwrap();
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
            self.log.write_all(command_json.as_bytes())?;
            self.log.seek(std::io::SeekFrom::Start(0))?;
            Ok(())
        } else {
            Err(failure::err_msg("Key not found"))
        }
    }
}

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
