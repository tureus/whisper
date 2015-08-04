use std::path::PathBuf;
use mmap::{Mmap, Protection};

mod header;
mod archive;

use self::header::Header;
use self::archive::Archive;

pub struct WhisperFile<'a> {
	pub path: PathBuf,
	pub mmap: Mmap,
	pub header: Header,
	pub archives: Vec< Archive<'a> >,
}

impl<'a> WhisperFile<'a> {
	pub fn open(path: PathBuf) -> WhisperFile<'a> {
		let mmap = Mmap::open(&path, Protection::ReadWrite).unwrap();
		let header = Header::new_from_slice(&mmap);

		let mut retval = WhisperFile {
			path: path,
			mmap: mmap,
			header: header,
			archives: vec![]
		};
		retval
	}

	pub fn setup_archives(&'a mut self) {
	}
}