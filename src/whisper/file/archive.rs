use std::rc::Rc;
use std::ops::Deref;

use mmap::Mmap;

pub struct Archive {
	seconds_per_point: u32,
	points: usize,

	mmap: Rc<Mmap>,
	begin: usize,
	end: Option<usize>
}

pub const ARCHIVE_INFO_SIZE : usize = 12;

impl Archive {
	pub fn new(seconds_per_point: u32, points: usize, mmap: Rc<Mmap>, begin: usize, end: Option<usize>) -> Archive {
		Archive {
			seconds_per_point: seconds_per_point,
			points: points,
			mmap: mmap,
			begin: begin,
			end: end
		}
	}
}

impl Deref for Archive {
	type Target = [u8];

	fn deref(&self) -> &[u8] {
		match self.end {
			Some( end ) => &self.mmap[ self.begin .. end ],
			None        => &self.mmap[ self.begin .. ]
		}
	}
}