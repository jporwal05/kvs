use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, Write},
    path::PathBuf,
    result,
};

use failure::Error;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

/// A [`Result`] that returns type [`T`] otherwise [`Error`]
pub type Result<T> = result::Result<T, Error>;

/// A container for storing key-value pairs in memory.
pub struct KvStore {
    index: HashMap<String, usize>,
    log: File,
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
            .open(path_buf)
            .unwrap();

        Ok(KvStore {
            log: file,
            index: HashMap::new(),
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
        self.log.write_all(command_json.as_bytes()).unwrap();
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
        let mut stream = Deserializer::from_reader(BufReader::new(&self.log)) // new line
            .into_iter::<Command>();
        let mut found = false;
        let mut value: Option<String> = None;

        let mut first = true;
        while let Some(Ok(c)) = stream.next() {
            if first {
                self.index.insert(key.to_string(), 0);
                first = false;
            } else {
                self.index.insert(key.to_string(), stream.byte_offset());
            }
            if c.key == key && c.command_type == CommandType::SET {
                found = true;
                value = c.value;
            }
            if c.key == key && c.command_type == CommandType::RM {
                found = false;
                value = None;
            }
        }

        if found {
            let command = Command {
                key: key.to_string(),
                value: None,
                command_type: CommandType::GET,
            };

            let command_json = serde_json::to_string(&command).unwrap();
            self.log.write_all(command_json.as_bytes()).unwrap();
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
        let mut stream = Deserializer::from_reader(BufReader::new(&self.log)) // new line
            .into_iter::<Command>();
        let mut found = false;
        let mut first = true;
        while let Some(Ok(c)) = stream.next() {
            if first {
                self.index.insert(key.to_string(), 0);
                first = false;
            } else {
                self.index.insert(key.to_string(), stream.byte_offset());
            }
            if c.key == key && c.command_type == CommandType::SET {
                found = true;
            }
            if c.key == key && c.command_type == CommandType::RM {
                found = false;
            }
        }

        if found {
            let command = Command {
                key: key.to_string(),
                value: None,
                command_type: CommandType::RM,
            };

            let command_json = serde_json::to_string(&command).unwrap();
            self.log.write_all(command_json.as_bytes()).unwrap();
            Ok(())
        } else {
            println!("Key not found");
            Err(failure::err_msg("Key not found"))
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Command {
    key: String,
    value: Option<String>,
    command_type: CommandType,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
enum CommandType {
    SET,
    GET,
    RM,
}
