use std::fmt;
use std::cmp;

use memmap::MmapView;
use byteorder::{ByteOrder, BigEndian, WriteBytesExt };
use std::io::{ Write, Cursor };

use super::super::point::{ self, Point }; // , POINT_SIZE_ON_DISK

// Index in to an archive, 0..points.len
#[derive(Debug, PartialEq, PartialOrd)]
pub struct ArchiveIndex(pub u32);

// A normalized timestamp. The thing you write in to the file.
#[derive(Debug, PartialEq)]
pub struct BucketName(pub u32);

pub struct Archive {
	seconds_per_point: u32,
	points: usize,

	mmap_view: MmapView
}

impl fmt::Debug for Archive {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "an archive")
    }
}

impl cmp::PartialEq for Archive {
	fn eq(&self, other: &Archive) -> bool {
		false
	}
	fn ne(&self, other: &Archive) -> bool {
		false
	}
}

// offset + seconds_per_point + points
pub const ARCHIVE_INFO_SIZE : usize = 12;

impl Archive {
	pub fn new(seconds_per_point: u32, points: usize, mmap_view: MmapView) -> Archive {

		Archive {
			seconds_per_point: seconds_per_point,
			points: points,
			mmap_view: mmap_view
		}
	}

	pub fn write(&mut self, point: Point ) {
		let bucket_name = self.bucket_name(point.0);
		let metric_value = point.1;

		let archive_index = self.archive_index(&bucket_name);

		let mut mmap_data = unsafe{ self.mmap_view.as_mut_slice() };
		let mut point_slice = &mut mmap_data[archive_index.0 as usize .. point::POINT_SIZE_ON_DISK];
		let mut writer = Cursor::new(point_slice);
		writer.write_u32::<BigEndian>(bucket_name.0).unwrap();
		writer.write_f64::<BigEndian>(metric_value).unwrap();
	}

	#[inline]
	pub fn seconds_per_point(&self) -> u32 {
		self.seconds_per_point
	}

	#[inline]
	pub fn points(&self) -> usize {
		self.points
	}

	#[inline]
	pub fn size(&self) -> usize {
		self.mmap_view.len()
	}

	#[inline]
    fn bucket_name(&self, timestamp: u32) -> BucketName {
        let bucket_name = timestamp - (timestamp % self.seconds_per_point);
        BucketName(bucket_name)
    }

    #[inline]
    fn archive_index(&self, bucket_name: &BucketName) -> ArchiveIndex {
    	// This line keep that first data page hot all the time.
    	// TODO: cache
    	let anchor_bucket_name = self.anchor_bucket_name();
    	if anchor_bucket_name.0 == 0 {
    		ArchiveIndex(0)
    	} else {
    		// let time_distance = bucket_name.0 - anchor_bucket_name.0;
    		// let distance_in_points = time_distance / self.seconds_per_point;
    		let point_distance = ( anchor_bucket_name.0 + bucket_name.0 ) % (self.points as u32);
    		ArchiveIndex(point_distance)
    	}
    }

    #[inline]
    fn anchor_bucket_name(&self) -> BucketName {
    	let first_four_bytes = BigEndian::read_u32(&unsafe{ self.mmap_view.as_slice()}[0..5]);
    	BucketName( first_four_bytes )
    }
}

#[cfg(test)]
mod tests {
	use super::*;
	use super::super::super::point::Point;
	use std::io::Cursor;
	use std::io::Write;
	use memmap::{ Mmap, Protection };

	// whisper-create.py blah.wsp 60:5
	// hexdump -v -e '"0x" 1/1 "%02X, "' blah.wsp
	const SAMPLE_FILE : [u8; 88] = [
	//  agg type
		0x00, 0x00, 0x00, 0x01,
	//  max ret
		0x00, 0x00, 0x01, 0x2C,
	// x_files_factor
		0x3F, 0x00, 0x00, 0x00,
	// archive_count
		0x00, 0x00, 0x00, 0x01,
	// archive_info[0].offset
		0x00, 0x00, 0x00, 0x1C,
	// archive_info[0].seconds_per_point
		0x00, 0x00, 0x00, 0x3C,
	// archive_info[0].points
		0x00, 0x00, 0x00, 0x05,
	// archive[0] data
		0x55, 0xD9, 0x33, 0xE8, 0x40, 0x59, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00
	];

	#[test]
	fn test_write(){
		let archive_data = &SAMPLE_FILE[28..];
		assert_eq!(archive_data[0], 0x55);

		let mut anon_mmap = Mmap::anonymous(archive_data.len(), Protection::ReadWrite).unwrap();
		{
			let slice : &mut [u8] = unsafe{ anon_mmap.as_mut_slice() };
			let mut cursor = Cursor::new(slice);
			cursor.write(&archive_data[..]).unwrap();
		};

		let anon_view = anon_mmap.into_view();
		let mut archive = Archive::new(60, 5, anon_view);
		assert_eq!(archive.anchor_bucket_name(), BucketName(1440297960) );
		assert_eq!(archive.seconds_per_point(), 60);
		assert_eq!(archive.points(), 5);
		assert_eq!(archive.size(), 60);
		assert_eq!(archive.archive_index(&BucketName(1440297960)), ArchiveIndex(0));

		let point = Point(1440297960,5.0);
		archive.write(point)
	}
}

// impl DerefMut for Archive {
// 	fn deref_mut(&mut self) -> &mut [u8] {
// 		// println!("supppp (unique: {}, strong_count {})", rc::is_unique(&self.mmap), rc::strong_count(&self.mmap) );
// 		match self.end {
// 			Some( end ) => &mut self.mmap.lock().unwrap()[ self.begin .. end ],
// 			None        => &mut rc::get_mut(&mut self.mmap).unwrap()[ self.begin .. ]
// 		}
// 	}
// }