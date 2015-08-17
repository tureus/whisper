use std::cell::RefCell;
use std::rc::Rc;
use mmap::MmapView;

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

pub const STATIC_HEADER_SIZE : usize = 12;

impl Header {
	pub fn new(_: u32, max_ret: u32, xff: f32) -> Header {
		Header {
			aggregation_type: AggregationType::Unknown,
			max_retention: max_ret,
			x_files_factor: xff
		}
	}

	pub fn new_from_slice(mmap_view: &MmapView) -> Header {
		let mmap_data = unsafe { mmap_view.as_slice() };
		let aggregation_type_u32 = BigEndian::read_u32(&mmap_data[0..4]);
		let max_retention = BigEndian::read_u32(&mmap_data[4..9]);
		let x_files_factor = BigEndian::read_f32(&mmap_data[8..13]);

		Header::new(aggregation_type_u32, max_retention, x_files_factor)
	}

	pub fn mmap_to_archives(&self, mmap_data: MmapView) -> Vec<Archive> {
		let archive_count = BigEndian::read_u32(&unsafe{ mmap_data.as_slice() }[12..17]) as usize;

		struct archive_info(u32,usize);
		let mut archive_infos : Vec<archive_info> = Vec::with_capacity(archive_count);
		{
			let ai_start = 16;
			let ai_end = 16 + archive::ARCHIVE_INFO_SIZE*archive_count;
			let archive_info_slice = &unsafe{ mmap_data.as_slice() }[ ai_start .. ai_end ];
			let chunks = archive_info_slice.chunks( archive::ARCHIVE_INFO_SIZE );

			for archive_info_slice in chunks {
				let _offset = BigEndian::read_u32(&archive_info_slice[0..4]);
				let seconds_per_point = BigEndian::read_u32(&archive_info_slice[4..9]);
				let points = BigEndian::read_u32(&archive_info_slice[8..]) as usize;
				archive_infos.push(archive_info(seconds_per_point,points))
			};
		}

		// chop off the header from the mmap
		let archive_start = STATIC_HEADER_SIZE + archive::ARCHIVE_INFO_SIZE*archive_count;
		let (header_view,mut archive_data) = mmap_data.split_at(archive_start).unwrap();

		// progressively cut down archive_data into each individual archive
		let mut archives : Vec<Archive> = Vec::with_capacity(archive_count);
		if archive_count > 1 {
			let (archives_init,archive_last) = archive_infos.split_at(archive_infos.len()-1);
			for info in archives_init {
				let offset = info.1 * point::POINT_SIZE_ON_DISK;
				let (this_archive,the_rest) = archive_data.split_at(offset).unwrap();
				archives.push(Archive::new(info.0, info.1, this_archive));
				archive_data = the_rest;
			}

			archives.push( Archive::new(archive_last[0].0, archive_last[0].1, archive_data));
		} else {

			let archive = Archive::new(archive_infos[0].0, archive_infos[0].1, archive_data);
			archives.push( archive )
		}

			// let mut archive_offset_cursor = ai_end;
			// chunks.each(|archive_info_chunk: &[u8]| {
			// 	let offset = BigEndian::read_u32(&archive_info_chunk[0..4]);
			// 	let seconds_per_point = BigEndian::read_u32(&archive_info_chunk[4..9]);
			// 	let points = BigEndian::read_u32(&archive_info_chunk[8..]) as usize;

			// 	let (archive_start, archive_end) = {
			// 		let archive_size = points*point::POINT_SIZE_ON_DISK;
			// 		let archive_end = archive_offset_cursor+archive_size+1;
			// 		if archive_end > mmap_data.borrow().len() {
			// 			(archive_offset_cursor, None)
			// 		} else {
			// 			(archive_offset_cursor, Some(archive_offset_cursor+archive_size+1) )
			// 		}
			// 	};
			// 	let slice = MmapSlice::new(mmap_data.clone(), archive_start, archive_end);
			// 	Archive::new(seconds_per_point, points, slice)
			// })
		// }

		archives
	}
}