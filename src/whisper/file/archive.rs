pub struct Archive<'a> {
	seconds_per_point: u32,
	points: usize,
	mmap_slice: &'a [u8]
}

pub const ARCHIVE_INFO_SIZE : usize = 12;

impl<'a> Archive<'a> {

	pub fn new(seconds_per_point: u32, points: usize, mmap_slice: &'a [u8]) -> Archive {
		Archive {
			seconds_per_point: seconds_per_point,
			points: points,
			mmap_slice: mmap_slice
		}
	}
}