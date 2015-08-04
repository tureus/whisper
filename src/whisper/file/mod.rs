use std::path::PathBuf;
use std::rc::Rc;

use mmap::{Mmap, Protection};

mod header;
mod archive;

use self::header::Header;
use self::archive::Archive;

pub struct WhisperFile {
	pub path: PathBuf,
	pub mmap: Rc<Mmap>,
	pub header: Header,
	pub archives: Vec< Archive >,
}

impl WhisperFile {
	pub fn open(path: PathBuf) -> WhisperFile {
		let mmap = Rc::new( Mmap::open(&path, Protection::ReadWrite).unwrap() );
		let header = Header::new_from_slice(&mmap);
		let archives = header.borrow_archives(&mmap);

		let mut retval = WhisperFile {
			path: path,
			mmap: mmap,
			header: header,
			archives: archives
		};
		retval
	}
}