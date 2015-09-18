use std::fmt;
use std::cmp;

use memmap::MmapViewSync;
use byteorder::{ByteOrder, BigEndian };

use whisper::Point;
use super::super::point::{ self };

// offset + seconds_per_point + points
pub const ARCHIVE_INFO_SIZE : usize = 12;

// Index in to an archive, 0..points.len
#[derive(Debug, PartialEq, PartialOrd)]
pub struct ArchiveIndex(pub u32);

// A normalized timestamp. The thing you write in to the file.
#[derive(Debug, PartialEq)]
pub struct BucketName(pub u32);

pub struct Archive {
	seconds_per_point: u32,
	points: usize,

	mmap_view: MmapViewSync
}

impl fmt::Debug for Archive {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Archive(seconds_per_point: {}, points: {})", self.seconds_per_point, self.points)
    }
}

impl Archive {
	pub fn new(seconds_per_point: u32, points: usize, mmap_view: MmapViewSync) -> Archive {

		Archive {
			seconds_per_point: seconds_per_point,
			points: points,
			mmap_view: mmap_view
		}
		
	}

	pub fn write(&mut self, point: &Point ) {
		let bucket_name = self.bucket_name(point.0);

		let archive_index = self.archive_index(&bucket_name);

		let start = archive_index.0 as usize * point::POINT_SIZE;
		let end = archive_index.0 as usize * point::POINT_SIZE + point::POINT_SIZE;

		let mut point_slice = &mut self.mut_slice()[start .. end];
		point.write_to_slice(bucket_name, point_slice);
	}

	pub fn read_points(&self, from: BucketName, points: &mut[Point]) {
		assert!(self.points() <= points.len(), "did not hold: {} <= {}", self.points(), points.len());
		let start = self.archive_index(&from);
		panic!("bucket: {}, start: {}", from.0, start.0);

		let mut data_needed = points.len()*point::POINT_SIZE as usize;

		let end_of_read = (start.0 as usize)*point::POINT_SIZE + data_needed;

		// Wrap around reads need two different passes
		if end_of_read > self.size() {
			panic!("end_of_read > self.size(): {} > {}", end_of_read, self.size());
			let overflow_bytes = end_of_read-self.size();

			let mut index = 0;
			let first_start = start.0 as usize * point::POINT_SIZE;
			let first_end = self.size();
			let first_data = &self.slice()[first_start .. first_end];

			let second_start = 0;
			let second_end = overflow_bytes;
			let second_data = &self.slice()[second_start .. second_end];

			for pt_data in first_data.chunks(point::POINT_SIZE) {
				points[index] = Point::new_from_slice(pt_data);
				index = index + 1;
			};
			for pt_data in second_data.chunks(point::POINT_SIZE) {
				points[index] = Point::new_from_slice(pt_data);
				index = index + 1;
			};
		} else {
			let start_index = start.0 as usize * point::POINT_SIZE;
			let end_index = end_of_read;

			let points_data = &self.slice()[start_index .. end_index];
			for (i,pt_data) in points_data.chunks(point::POINT_SIZE).enumerate() {
				panic!("hey");
				println!("pt_data: 0x{:x}{:x}{:x}{:x}", pt_data[0], pt_data[1], pt_data[2], pt_data[3]);
				// TODO: should we instead pass the point in to the constructor?
				points[i] = Point::new_from_slice(pt_data)
			};
		};
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
    fn bucket(&self, timestamp: u32) -> BucketName {
        let bucket_name = timestamp - (timestamp % self.seconds_per_point);
        BucketName(bucket_name)
    }

    #[inline]
    fn archive_index(&self, bucket_name: &BucketName) -> ArchiveIndex {
    	// This line unnecessarily keeps that first data page hot all the time.
    	// TODO: cache
    	let anchor_bucket_name = self.anchor_bucket_name();
    	if anchor_bucket_name.0 == 0 {
    		ArchiveIndex(0)
    	} else {
    		let time_distance = bucket_name.0 + anchor_bucket_name.0;
    		// let distance_in_points = time_distance / self.seconds_per_point;
    		// let point_distance = ( anchor_bucket_name.0 - bucket_name.0 ) % (self.points as u32);
    		let point_distance = time_distance / self.seconds_per_point;
    		// panic!("({}-{}) % {} = {}", anchor_bucket_name.0, bucket_name.0, self.points, point_distance);
    		let index = Archive::py_mod(point_distance, self.points as u32);
    		ArchiveIndex(index)
    	}
    }

    fn py_mod(input: u32, base: u32) -> u32 {
        let remainder = input as i64 % base as i64;

        if remainder < 0 {
            (base as i64 + remainder) as u32
        } else {
            (remainder) as u32
        }
    }

    #[inline]
    pub fn anchor_bucket_name(&self) -> BucketName {
    	let first_four_bytes = BigEndian::read_u32(&self.slice()[0..5]);
    	BucketName( first_four_bytes )
    }

    #[inline]
    fn slice(&self) -> &[u8] {
		unsafe{ self.mmap_view.as_slice() }
    }

    #[inline]
    fn mut_slice(&mut self) -> &mut [u8] {
		unsafe{ self.mmap_view.as_mut_slice() }
    }
}

#[cfg(test)]
mod tests {
	use super::*;
	use super::super::super::point::Point;
	use std::io::Cursor;
	use std::io::Write;
	use memmap::{ Mmap, Protection };

	// ruby -e "%Q{`hexdump -v -e '"0x" 1/1 "%02X, "' blah.wsp`}.split(', ').each_slice(4){|arr| puts arr.join(',') + ',' }"
	const SAMPLE_FILE_2 : [u8; 64] = [
		0x00,0x00,0x00,0x01,
		0x00,0x00,0x00,0x06,
		0x3F,0x00,0x00,0x00,
		0x00,0x00,0x00,0x01,
		0x00,0x00,0x00,0x1C,
		0x00,0x00,0x00,0x02,
		0x00,0x00,0x00,0x03,
		0x55,0xDA,0xA3,0x98,
		0x40,0x59,0x00,0x00,
		0x00,0x00,0x00,0x00,
		0x55,0xDA,0xA3,0x9A,
		0x40,0x59,0x00,0x00,
		0x00,0x00,0x00,0x00,
		0x55,0xDA,0xA3,0x9C,
		0x40,0x59,0x00,0x00,
		0x00,0x00,0x00,0x00,
	];

	#[cfg(test)]
	fn build_mmap() -> Mmap{
		let archive_data = &SAMPLE_FILE_2[28..];
		assert_eq!(archive_data[0], 0x55);

		let mut anon_mmap = Mmap::anonymous(archive_data.len(), Protection::ReadWrite).unwrap();
		{
			let slice : &mut [u8] = unsafe{ anon_mmap.as_mut_slice() };
			let mut cursor = Cursor::new(slice);
			cursor.write(&archive_data[..]).unwrap();
		};

		anon_mmap
	}

	#[test]
	fn test_archive_index(){
		let anon_view = build_mmap().into_view_sync();

		let archive = Archive::new(2, 3, anon_view);

		// Our bucket names are aligned
		assert_eq!(archive.bucket(1440392088).0, 1440392088);
		assert_eq!(archive.bucket(1440392090).0, 1440392090);
		assert_eq!(archive.bucket(1440392092).0, 1440392092);

		assert_eq!(archive.archive_index(&BucketName(1440392088)).0, 0);
		assert_eq!(archive.archive_index(&BucketName(1440392090)).0, 1);
		assert_eq!(archive.archive_index(&BucketName(1440392092)).0, 2);

		// Now wrap around going down
		assert_eq!(archive.archive_index(&BucketName(1440392086)).0, 2);
		assert_eq!(archive.archive_index(&BucketName(1440392084)).0, 1);
		assert_eq!(archive.archive_index(&BucketName(1440392082)).0, 0);

		// Wrap around going up
		assert_eq!(archive.archive_index(&BucketName(1440392094)).0, 0);
		assert_eq!(archive.archive_index(&BucketName(1440392096)).0, 1);
		assert_eq!(archive.archive_index(&BucketName(1440392098)).0, 2);
	}

	// #[test]
	// fn test_read(){
	// 	let mut anon_view = build_mmap().into_view_sync();
	// 	let mut archive = Archive::new(2, 3, anon_view);
	// 	assert_eq!(archive.anchor_bucket_name(), BucketName(1440392088) );
	// 	assert_eq!(archive.seconds_per_point(), 2);
	// 	assert_eq!(archive.points(), 3);
	// 	assert_eq!(archive.size(), 36);
	// 	assert_eq!(archive.archive_index(&BucketName(1440392088)), ArchiveIndex(0));

	// 	{
	// 		let mut points_buf = Vec::with_capacity(3);
	// 		unsafe{ points_buf.set_len(3) };
	// 		archive.read_points(BucketName(0), &mut points_buf[..]);		
	// 		let expected = vec![
	// 			Point(1440392088, 100.0),
	// 			Point(1440392090, 100.0),
	// 			Point(1440392092, 100.0)
	// 		];
	// 		assert_eq!(points_buf, expected);

	// 		let point = Point(1440392090,8.0);
	// 		let bucket_name = BucketName(point.0);
	// 		archive.write(&point);
	// 		assert_eq!(archive.archive_index(&bucket_name).0, 1);

	// 		unsafe{ points_buf.set_len(1) };
	// 		archive.read_points(bucket_name, &mut points_buf[..]);		
	// 		assert_eq!(points_buf[0].0, 1440392090);
	// 		assert_eq!(points_buf[0].1, 8.0);
	// 	}
	// }
}
