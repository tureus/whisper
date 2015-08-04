use byteorder::{ ByteOrder, BigEndian, ReadBytesExt };

use super::archive::{ self, Archive };
use super::super::point;

pub enum AggregationType {
	Unknown
}

pub struct Header {
	aggregation_type: AggregationType,
	max_retention: u32,
	x_files_factor: f32,
}

impl Header {
	pub fn new(_: u32, max_ret: u32, xff: f32) -> Header {
		Header {
			aggregation_type: AggregationType::Unknown,
			max_retention: max_ret,
			x_files_factor: xff
		}
	}

	pub fn new_from_slice(mmap_data: &[u8]) -> Header {
		let aggregation_type_u32 = BigEndian::read_u32(&mmap_data[0..4]);
		let max_retention = BigEndian::read_u32(&mmap_data[4..9]);
		let x_files_factor = BigEndian::read_f32(&mmap_data[8..13]);

		Header::new(aggregation_type_u32, max_retention, x_files_factor)
	}

	pub fn borrow_archives<'a>(&self, mmap_data: &'a [u8]) -> Vec<Archive<'a>> {
		let archive_count = BigEndian::read_u32(&mmap_data[12..17]) as usize;

		let archives : Vec<Archive> = {
			let ai_start = 16;
			let ai_end = 16 + archive::ARCHIVE_INFO_SIZE*archive_count;
			let archive_info_slice = &mmap_data[ ai_start .. ai_end ];
			let chunks = archive_info_slice.chunks( archive::ARCHIVE_INFO_SIZE );

			let mut archive_offset_cursor = ai_end;
			chunks.map(|archive_info_chunk: &[u8]| {
				let offset = BigEndian::read_u32(&archive_info_chunk[0..4]);
				let seconds_per_point = BigEndian::read_u32(&archive_info_chunk[4..9]);
				let points = BigEndian::read_u32(&archive_info_chunk[8..]) as usize;

				let archive_slice = {
					let archive_size = points*point::POINT_SIZE_ON_DISK;
					let archive_end = archive_offset_cursor+archive_size+1;
					let archive_slice = if archive_end > mmap_data.len() {
						&mmap_data[ archive_offset_cursor ..]
					} else {
						&mmap_data[ archive_offset_cursor .. archive_offset_cursor+archive_size+1 ]
					};
					archive_slice
				};

				Archive::new(seconds_per_point, points, archive_slice)
			}).collect()
		};

		archives
	}
}