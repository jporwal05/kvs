# kvs
An in-memory key-value store developed in store in rust. This is backed by a Write Ahead Log(WAL). It can be used as a library as well as a CLI.

## pre-requisites
Have rust installed.

## build
Run `cargo build` in the root directory.

## docs
Run `cargo doc --open`

## tests
- Run `cargo test --doc` for documentation tests
- Run `cargo test` for implementation tests

`Log compaction` is not implemented yet so test named `compaction` will fail.

## usage as CLI
- `cargo run set key1 value1`
- `cargo run get key1`
- `cargo run rm key1`

Inspect file `kvs.store` created in the project root to see what is happening after each command. This is the Write Ahead Log(WAL).