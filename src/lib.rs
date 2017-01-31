#![cfg_attr(test, feature(test))]

extern crate memmap;
extern crate byteorder;
extern crate regex;
extern crate libc;
#[macro_use] extern crate error_chain;
#[cfg(test)] extern crate test;
#[cfg(test)] #[macro_use] extern crate assert_matches;
extern crate lru_cache;

#[macro_use]
extern crate log;

mod whisper;

pub use whisper::errors;
pub use self::whisper::{ WhisperFile, Point, Schema, WhisperCache, NamedPoint };
