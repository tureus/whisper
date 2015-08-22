use std::path::{ Path, PathBuf };

use memmap::{Mmap, Protection};

mod header;
mod archive;

use self::header::Header;
use self::archive::Archive;
use super::point::Point;

#[derive(Debug)]
pub struct WhisperFile {
	pub path: PathBuf,
	pub header: Header,
	pub archives: Vec< Archive >,
}

impl WhisperFile {
	pub fn open(path: &Path) -> WhisperFile {
		let mmap = Mmap::open_path(path, Protection::ReadWrite).unwrap();
		let mmap_view = mmap.into_view();

		let header = {
			let slice = unsafe{ mmap_view.as_slice() };
			Header::new_from_slice(slice)
		};
		let archives = header.mmap_to_archives(mmap_view);

		let whisper_file = WhisperFile {
			path: path.to_path_buf(),
			header: header,
			archives: archives
		};
		whisper_file
	}

	pub fn write(&mut self, point: Point) {
		self.archives[0].write(point);
	}
}