use std::process::exit;

use clap::crate_version;
use clap::{Arg, Command};

fn main() {
    let matches = Command::new("kvs")
        .version(crate_version!())
        .args([Arg::new("arg1"), Arg::new("arg2"), Arg::new("arg3")])
        .get_matches();
    if !matches.args_present() {
        exit(-1)
    }

    if let Some(arg1) = matches.get_one::<String>("arg1") {
        if arg1 == &"get".to_string() {
            match matches.get_one::<String>("arg2") {
                Some(_) => {
                    eprint!("unimplemented");
                    exit(-1);
                }
                None => panic!(),
            }
        } else if arg1 == &"set".to_string() {
            match matches.get_one::<String>("arg2") {
                Some(_) => match matches.get_one::<String>("arg3") {
                    Some(_) => {
                        eprint!("unimplemented");
                        exit(-1);
                    }
                    None => panic!(),
                },
                None => panic!(),
            }
        } else if arg1 == &"rm".to_string() {
            match matches.get_one::<String>("arg2") {
                Some(_) => {
                    eprint!("unimplemented");
                    exit(-1);
                }
                None => panic!(),
            }
        } else {
            panic!()
        }
    }
}
