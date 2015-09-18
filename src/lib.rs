#![feature(test, path_ext, dir_builder)]

extern crate memmap;
extern crate byteorder;
extern crate regex;
extern crate libc;
extern crate test;

mod whisper;

pub use self::whisper::{ WhisperFile, Point, Schema, WhisperCache, NamedPoint };
