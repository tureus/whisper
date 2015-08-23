use memmap::MmapView;

use byteorder::{ ByteOrder, BigEndian, ReadBytesExt };

use super::archive::{ self, Archive };
use super::super::point;

#[derive(Debug, PartialEq)]
pub enum AggregationType {
	Unknown
}

#[derive(Debug)]
pub struct Header {
	_aggregation_type: AggregationType,
	_max_retention: u32,
	_x_files_factor: f32,
}

pub const STATIC_HEADER_SIZE : usize = 12;

// Hey hey, now this info junk is only internal. How nice!
// Retention, points
// (Seconds Per Point, Points)
struct ArchiveInfo(u32,usize);

impl Header {
	pub fn new(_: u32, max_ret: u32, xff: f32) -> Header {
		Header {
			_aggregation_type: AggregationType::Unknown,
			_max_retention: max_ret,
			_x_files_factor: xff
		}
	}

	pub fn new_from_slice(mmap_data: &[u8]) -> Header {
		let aggregation_type_u32 = BigEndian::read_u32(&mmap_data[0..4]);
		let max_retention = BigEndian::read_u32(&mmap_data[4..9]);
		let x_files_factor = BigEndian::read_f32(&mmap_data[8..13]);

		Header::new(aggregation_type_u32, max_retention, x_files_factor)
	}

	fn archive_count(mmap_data: &[u8]) -> usize {
		BigEndian::read_u32(&mmap_data[12..17]) as usize
	}

	// Consumes MmapView to create Archives
	pub fn mmap_to_archives(mmap_data: MmapView) -> Vec<Archive> {
		let (archive_infos, archive_count) = {
			let raw_data = &unsafe{ mmap_data.as_slice() }; // localize not safe stuff
			let count = Header::archive_count(raw_data);
			let infos = Header::archive_infos(count, raw_data);
			(infos, count)
		};

		// chop off the header and throw it away, we're done with it
		let archive_start = STATIC_HEADER_SIZE + archive::ARCHIVE_INFO_SIZE*archive_count;
		let (_,mut archive_data) = mmap_data.split_at(archive_start).unwrap();

		let mut archives : Vec<Archive> = Vec::with_capacity(archive_count);
		// use infos to progressively cut down archive_data into each individual archive
		// or in the simple case of 1 archive just use the rest of the file
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
			archives.push( archive );
		}

		archives
	}

	fn archive_infos(archive_count: usize, all_header_data: &[u8]) -> Vec<ArchiveInfo> {
		let mut archive_infos : Vec<ArchiveInfo> = Vec::with_capacity(archive_count);

		let ai_start = 16;
		let ai_end = 16 + archive::ARCHIVE_INFO_SIZE*archive_count;

		let chunks = {
			let archive_info_slice = &all_header_data[ ai_start .. ai_end ];
			archive_info_slice.chunks( archive::ARCHIVE_INFO_SIZE )
		};

		for archive_info_slice in chunks {
			// we don't use offset because of how the MmapView is consumed in to smaller MmapViews
			// let _offset = BigEndian::read_u32(&archive_info_slice[0..4]);
			let seconds_per_point = BigEndian::read_u32(&archive_info_slice[4..9]);
			let points = BigEndian::read_u32(&archive_info_slice[8..]) as usize;
			archive_infos.push(ArchiveInfo(seconds_per_point,points));
		}

		archive_infos
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::io::Cursor;
	use std::io::Write;
	use memmap::{ Mmap, Protection };

	// whisper-create.py blah.wsp 60:5
	// hexdump -v -e '"0x" 1/1 "%02X, "' blah.wsp
	const SAMPLE_FILE : [u8; 88] = [
		0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 0x2C,
		0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x1C, 0x00, 0x00, 0x00, 0x3C,
		0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
	];

	#[test]
	fn test_header_from_slice() {
		let hdr = Header::new_from_slice(&SAMPLE_FILE[..]);
		assert_eq!(hdr._aggregation_type, AggregationType::Unknown);
		assert_eq!(hdr._max_retention, 300);
		assert_eq!(hdr._x_files_factor, 0.5);
		assert_eq!(Header::archive_count(&SAMPLE_FILE[..]), 1);

		println!("parsed_header: {:?}", hdr);
	}

	#[test]
	fn test_header_info() {
		let infos = Header::archive_infos(1, &SAMPLE_FILE[..]);
		assert_eq!(infos.len(), 1);
		let info = &infos[..][0];
		assert_eq!(info.0, 60);
		assert_eq!(info.1, 5);
	}

	#[test]
	fn test_mmap_to_archives(){
		let mut anon_mmap = Mmap::anonymous(SAMPLE_FILE.len(), Protection::ReadWrite).unwrap();
		{
			let mut slice : &mut [u8] = unsafe{ anon_mmap.as_mut_slice() };
			let mut cursor = Cursor::new(slice);
			cursor.write(&SAMPLE_FILE[..]).unwrap();			
		}

		let mmap_view = anon_mmap.into_view();
		let archives = Header::mmap_to_archives(mmap_view);
		assert_eq!(archives.len(), 1);
	}
}