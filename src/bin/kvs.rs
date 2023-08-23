use std::process::exit;

use clap::crate_version;
use clap::{Arg, Command};
use kvs::{KvStore, Result};

fn main() -> Result<()> {
    let matches = Command::new("kvs")
        .version(crate_version!())
        .args([Arg::new("arg1"), Arg::new("arg2"), Arg::new("arg3")])
        .get_matches();
    if !matches.args_present() {
        exit(-1)
    }

    if let Some(arg1) = matches.get_one::<String>("arg1") {
        if arg1 == &"get".to_string() {
            let extra_field = matches.contains_id("arg3");
            if extra_field {
                panic!()
            }
            match matches.get_one::<String>("arg2") {
                Some(arg2) => {
                    let mut store = KvStore::open(".").unwrap();
                    match store.get(arg2.to_string()) {
                        Ok(value) => match value {
                            Some(_) => (),
                            None => println!("Key not found"),
                        },
                        Err(_) => (),
                    }
                }
                None => panic!(),
            }
        } else if arg1 == &"set".to_string() {
            match matches.get_one::<String>("arg2") {
                Some(arg2) => match matches.get_one::<String>("arg3") {
                    Some(arg3) => {
                        let mut store = KvStore::open(".").unwrap();
                        store.set(arg2.to_string(), arg3.to_string()).unwrap();
                    }
                    None => panic!(),
                },
                None => panic!(),
            }
        } else if arg1 == &"rm".to_string() {
            match matches.get_one::<String>("arg2") {
                Some(arg2) => {
                    let mut store = KvStore::open(".").unwrap();
                    match store.remove(arg2.to_string()) {
                        Ok(_) => (),
                        Err(_) => exit(1),
                    }
                }
                None => panic!(),
            }
        } else {
            panic!()
        }
    }

    Ok(())
}
