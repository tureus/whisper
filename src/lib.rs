#![feature(alloc)]
#![feature(rc_unique, rc_counts)]

extern crate mmap;
extern crate byteorder;

mod whisper;

pub use self::whisper::{ WhisperFile, Point };