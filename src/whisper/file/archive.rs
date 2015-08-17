use std::cell::{ RefCell, Ref };
use std::rc::Rc;

use std::ops::{ Deref /*,  DerefMut */ };
use std::io::Cursor;

use mmap::MmapView;
use byteorder::{ByteOrder, BigEndian, WriteBytesExt};

use super::super::point::{Point, POINT_SIZE_ON_DISK};

// Index in to an archive, 0..points.len
#[derive(Debug, PartialEq, PartialOrd)]
pub struct ArchiveIndex(pub u32);

// A normalized timestamp. The thing you write in to the file.
pub struct BucketName(pub u32);

pub struct Archive {
	seconds_per_point: u32,
	points: usize,

	mmap_view: MmapView
}

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

		unimplemented!();
		// {
		// 	let dem_bytes : &mut[u8] = self.bytes_for_point_at_mut(&archive_index);
		// 	let mut writer = Cursor::new(dem_bytes);
		// 	writer.write_u32::<BigEndian>(bucket_name.0).unwrap();
		// 	writer.write_f64::<BigEndian>(metric_value).unwrap();
		// }
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

	// fn bytes_for_point_at_mut(&mut self, ai: &ArchiveIndex) -> &mut [u8] {
	// 	let point_start = (ai.0 as usize) * POINT_SIZE_ON_DISK;
	// 	let point_end = point_start + POINT_SIZE_ON_DISK;
	// 	if point_end > self.mmap.borrow().len() {
	// 		&mut self[ point_start .. ]
	// 	} else {
	// 		&mut self[ point_start .. point_end ]
	// 	}
	// }

	// fn bytes_for_point_at(&self, ai: &ArchiveIndex) -> Ref<&[u8]> {
	// 	let point_start = (ai.0 as usize) * POINT_SIZE_ON_DISK;
	// 	let point_end = point_start + POINT_SIZE_ON_DISK;
	// 	let len = self.mmap_view.len();
	// 	if point_end > len {
	// 		&unsafe{ self.mmap_view.as_slice() }[ point_start .. ]
	// 	} else {
	// 		&unsafe{ self.mmap_view.as_slice() }[ point_start .. point_end ]
	// 	}
	// }



	// pub fn write(&self, ai: ArchiveIndex, (_, _): (BucketName,f64)) {
	// 	let _ = &self[ai];
	// }
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