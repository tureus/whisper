#![cfg_attr(test, feature(test))]

extern crate memmap;
extern crate byteorder;
extern crate regex;
extern crate libc;
#[cfg(test)] extern crate test;
extern crate lru_cache;

#[macro_use]
extern crate log;

mod whisper;

pub use self::whisper::{ WhisperFile, Point, Schema, WhisperCache, NamedPoint };
