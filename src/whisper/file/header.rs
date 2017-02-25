use std::fmt;

use memmap::MmapViewSync;
use byteorder::{ ByteOrder, BigEndian };

use super::archive::{ self, Archive };
use super::super::point;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum AggregationType {
    Average = 1,
    Sum = 2
}

impl AggregationType {
    pub fn aggregate(&self, points: &[point::Point]) -> f64 {
        match *self {
            AggregationType::Average => {
                if points.is_empty() { return 0.0 };
                let count = points.len() as f64;
                let sum: f64 = points.iter().map(point::Point::value).sum();
                sum / count
            },
            AggregationType::Sum => points.iter().map(point::Point::value).sum()
        }
    }
}

impl fmt::Display for AggregationType {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			AggregationType::Average => write!(f, "average"),
			AggregationType::Sum => write!(f, "sum")
		}
	}
}

impl AggregationType {
	pub fn from_u32(val: u32) -> AggregationType {
		match val {
			2 => AggregationType::Sum,
			_  => AggregationType::Average
		}
	}
}

#[derive(Debug)]
pub struct Header {
	pub aggregation_type: AggregationType,
	pub max_retention: u32,
	pub x_files_factor: f32,
}

pub const STATIC_HEADER_SIZE : usize = 16;

// Hey hey, now this info junk is only internal. How nice!
// Retention, points
// (Seconds Per Point, Points)
struct ArchiveInfo(u32,usize);

impl Header {
	pub fn new_from_slice(mmap_data: &[u8]) -> Header {
		let aggregation_type_u32 = BigEndian::read_u32(&mmap_data[0..4]);
		let max_retention = BigEndian::read_u32(&mmap_data[4..9]);
		let x_files_factor = BigEndian::read_f32(&mmap_data[8..13]);

		let agg_type = AggregationType::from_u32(aggregation_type_u32);

		Header::new(agg_type, max_retention, x_files_factor)
	}

	pub fn new(agg_type: AggregationType, max_ret: u32, xff: f32) -> Header {
		Header {
			aggregation_type: agg_type,
			max_retention: max_ret,
			x_files_factor: xff
		}
	}

	#[inline]
	fn archive_count(mmap_data: &[u8]) -> usize {
		BigEndian::read_u32(&mmap_data[12..17]) as usize
	}

	#[inline]
	pub fn archives_start(archive_count: usize) -> usize {
		STATIC_HEADER_SIZE + archive::ARCHIVE_INFO_SIZE*archive_count
	}

	#[inline]
	pub fn aggregation_type(&self) -> AggregationType {
		self.aggregation_type.clone()
	}

	#[inline]
	pub fn max_retention(&self) -> u32 {
		self.max_retention
	}

	#[inline]
	pub fn x_files_factor(&self) -> f32 {
		self.x_files_factor
	}

	// Consumes MmapViewSync to create Archives with smaller MmapViewSync
	pub fn mmap_to_archives(&self, mmap_data: MmapViewSync) -> Vec<Archive> {
		let (archive_infos, archive_count) = {
			let raw_data = &unsafe{ mmap_data.as_slice() }; // localize not safe stuff
			let count = Header::archive_count(raw_data);
			let infos = Header::archive_infos(count, raw_data);
			(infos, count)
		};

		// chop off the header and throw it away, we're done with it
		let start = Header::archives_start(archive_count);
		let (_,mut archive_data) = mmap_data.split_at(start).unwrap();

		let mut archives : Vec<Archive> = Vec::with_capacity(archive_count);
		// use infos to progressively cut down archive_data into each individual archive
		// or in the simple case of 1 archive just use the rest of the file
		if archive_count > 1 {

			let (archives_init,archive_last) = archive_infos.split_at(archive_infos.len()-1);
			for info in archives_init {
				let offset = info.1 * point::POINT_SIZE;
				let (this_archive,the_rest) = archive_data.split_at(offset).unwrap();

				assert!(this_archive.len() != 30, "this_archive.len(): {}, the_rest.len(): {}",this_archive.len(),the_rest.len());

				archives.push(Archive::new(info.0, info.1, this_archive));
				archive_data = the_rest;
			}

			assert!(archive_data.len() != 30, "this_archive.len(): {}",archive_data.len());
			archives.push( Archive::new(archive_last[0].0, archive_last[0].1, archive_data));

		} else {

			let archive = Archive::new(archive_infos[0].0, archive_infos[0].1, archive_data);
			archives.push( archive );
		}

		archives
	}

	fn archive_infos(archive_count: usize, all_header_data: &[u8]) -> Vec<ArchiveInfo> {
		let mut archive_infos : Vec<ArchiveInfo> = Vec::with_capacity(archive_count);

		let ai_start = STATIC_HEADER_SIZE;
		let ai_end = STATIC_HEADER_SIZE + archive::ARCHIVE_INFO_SIZE*archive_count;

		let chunks = {
			let archive_info_slice = &all_header_data[ ai_start .. ai_end ];
			archive_info_slice.chunks( archive::ARCHIVE_INFO_SIZE )
		};

		for archive_info_slice in chunks {
			// we don't use offset because of how the MmapViewSync is consumed in to smaller MmapViewSyncs
			// let _offset = BigEndian::read_u32(&archive_info_slice[0..4]);
			let seconds_per_point = BigEndian::read_u32(&archive_info_slice[4..9]);
			let points = BigEndian::read_u32(&archive_info_slice[8..]) as usize;
			archive_infos.push(ArchiveInfo(seconds_per_point,points));
		}

		archive_infos
	}
}
