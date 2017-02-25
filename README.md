Whisper
=====

A turbo-charged whisper database implementation.

Open tasks
----

 - [x] `WhisperCache`
 - [ ] Validate `whisper-dump.py` behavior
 - [x] Aggregations on write
 - [ ] `SchemaRegistry` or similar
 - [ ] Validate retention policies in schema
 - [ ] Validate WhisperFile when opening
 - [ ] tmpfile support in test cases
 - [ ] test suite comparing behavior with python implementation
 - [ ] WhisperFile advisory locking
 - [ ] WhisperFile#write to io::Result (file deleted while app is running removes from cache, etc)
 - [ ] Put most #[derive(Debug)] behind feature flag for test mode only?
 - [ ] What should we do when we get a 'nan' value in a datagram? Right now it goes to 0.0.

What is Whisper?
----

Whisper is fixed-size file format for storing one run of time series measurements. A measurement comes from a single instance of a thing, said as `CPU0 on Computer A` or `Bytes Transmitted on eth0 for Computer B`. To measure a system you will end up with multiple whisper files.

The fixed-size of the file means it has a fixed retention, it can only store so many measurements. Each measurement has a timestamp which corresponds to a predetermined location in the file. And when you get to the end of the file the location just wraps around and overwrites data.

Whisper has clear benefits: easy capacity planning, no dynamic allocations. Whisper-files are the simplest way of storing time series data. This simplicity certainly has its tradeoffs but provides the best raw performance and throughput.

Note: if you want a more modern, clustered, appending time series database it is highly recommended you explore [InfluxDB](https://www.influxdb.org/). It's still under heavy development but reflects the future we want.

The Python Implemenation
---

The original whisper system is written in Python, [check out the project on github](https://github.com/graphite-project/whisper). There's a severe lack of tests, the code has multiple unused variables, and the most interesting parts are large, undocumented methods.

This Rust Implementation
---

This is actually version 2 of Xavier's reimplementation. The aim is create a small, fast library which can become the kernel of a full graphite implementation. This maintains full backwards compatibility with your existing whisper files.

How is it faster?

 - Staticly compiled to machine code with few dynamic allocations, no garbage collection, and no runtime
 - Uses POSIX facilities for `mmap`, commonly read pages of files are kept hot in RAM
 - Files are kept open for longer with the read-through `WhisperCache`

How is the code better?

 - Functionality is broken out to smaller, unit tested rust modules
 - Whisper concepts are encoded in types to avoid programmer error and keep things explicit

How do I use it?
----

`git clone https://github.com/tureus/whisper-mmap` and `cargo test` to verify things work ok.

Simply opening a whisper file:

```
let path = "/tmp/blah.wsp";
let default_specs = vec!["1s:60s".to_string(), "1m:1y".to_string()];
let schema = Schema::new_from_retention_specs(default_specs).unwrap();

let file = WhisperFile::new(path, schema, AggregationType::Sum, 0.0).unwrap();
// do things with the file
```
