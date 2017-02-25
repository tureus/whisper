use memmap::{ Mmap, Protection };
use byteorder::{ BigEndian, WriteBytesExt };
use time;

mod header;
pub mod archive;

use self::header::Header;
use self::archive::Archive;

pub use self::header::{STATIC_HEADER_SIZE, AggregationType};
pub use self::archive::ARCHIVE_INFO_SIZE;

use whisper::Point;
use whisper::Schema;

// Modules needed to create file on disk
use std::fs::OpenOptions;
extern crate libc;
use self::libc::ftruncate;
use std::os::unix::prelude::AsRawFd;
use std::io::{ self, Error};
use std::path::{ Path, PathBuf };
use std::fmt;
use std::cmp;
use std::iter::repeat;

pub struct WhisperFile {
	pub path: PathBuf,
	pub header: Header,
	pub archives: Vec< Archive >,
}

impl fmt::Debug for WhisperFile {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		try!(write!(f, "Meta data:
  aggregation method: {}
  max retention: {}
  xFilesFactor: {}

", self.header.aggregation_type, self.header.max_retention, self.header.x_files_factor));

		let mut index = 0;
		let mut offset = Header::archives_start(self.archives.len());

		let max_points = self.archives.iter().map(|x| x.points()).max().unwrap();
		let mut points_buf = Vec::with_capacity(max_points);

		for archive in &self.archives {
			try!(write!(f, "Archive {} info:
  offset: {}
  seconds per point: {}
  points: {}
  retention: {}
  size: {}

Archive {} data:
", index, offset, archive.seconds_per_point(), archive.points(), archive.seconds_per_point() * archive.points() as u32, archive.size(), index ));

			unsafe{ points_buf.set_len(archive.points()) };
			try!(archive.read_points(archive.anchor_bucket_name(), &mut points_buf).map_err(|_| fmt::Error));

			let mut points_index = 0;
			for point in &points_buf {
				try!(write!(f, "{}:	{},          {}\n", points_index, point.0, point.1));

				points_index = points_index + 1;
			}

			offset = offset + archive.size();
			index = index + 1;
		}

		write!(f,"")
	}
}

impl WhisperFile {
	pub fn new<P>(path: P, schema: &Schema, agg: AggregationType, xff: f32) -> io::Result<WhisperFile>
        where P: AsRef<Path> {
		let mut opened_file = try!(OpenOptions::new().read(true).write(true).create(true).open(path.as_ref()));

		// Allocate space on disk (could be costly!)
		{
			let size_needed = schema.size_on_disk();
			let raw_fd = opened_file.as_raw_fd();
			let retval = unsafe {
				// TODO skip to fallocate-like behavior. Will need wrapper for OSX.
				ftruncate(raw_fd, size_needed as i64)
			};
			if retval != 0 {
				return Err(Error::last_os_error());
			}
		}

		let header = Header::new(agg, schema.max_retention(), xff);
		{
			try!( opened_file.write_u32::<BigEndian>( header.aggregation_type as u32));
			try!( opened_file.write_u32::<BigEndian>( header.max_retention ) );
			try!( opened_file.write_f32::<BigEndian>( header.x_files_factor ) );
			try!( opened_file.write_u32::<BigEndian>( schema.retention_policies.len() as u32 ) );
		}

		let mut archive_offset = Header::archives_start( schema.retention_policies.len() ) as u32;
		for retention_policy in &schema.retention_policies {
			try!( opened_file.write_u32::<BigEndian>( archive_offset as u32 ) );
			try!( opened_file.write_u32::<BigEndian>( retention_policy.precision ) );
			try!( opened_file.write_u32::<BigEndian>( retention_policy.points()  ) );

			archive_offset = archive_offset + retention_policy.size_on_disk();
		}

		let mmap = Mmap::open(&opened_file, Protection::ReadWrite ).unwrap();

		Ok( WhisperFile::open_mmap(path.as_ref(), mmap) )
	}

	// TODO: open should validate contents of whisper file
	// and return Result<WhisperFile, io::Error>
	pub fn open<P>(path: P) -> WhisperFile
        where P: AsRef<Path> {
		let mmap = Mmap::open_path(path.as_ref(), Protection::ReadWrite).unwrap();
		WhisperFile::open_mmap(path.as_ref(), mmap)
	}

	fn open_mmap<P>(path: P, mmap: Mmap) -> WhisperFile
	where P: AsRef<Path> {
		let mmap_view = mmap.into_view_sync();

		let header = {
			let slice = unsafe{ mmap_view.as_slice() };
			Header::new_from_slice(slice)
		};
		let archives = header.mmap_to_archives(mmap_view);

		let whisper_file = WhisperFile {
			path: path.as_ref().to_path_buf(),
			header: header,
			archives: archives
		};
		whisper_file
	}

        pub fn write(&mut self, point: &Point) {
            let now = time::get_time().sec;
            self._write(point, now)
        }

	fn _write(&mut self, point: &Point, now: i64) {
            let mut point = point.clone();
            let elapsed = now - point.0 as i64;
            if elapsed < 0 || elapsed as u32 >= self.header.max_retention() { return; }

            enum WriteState {
              Initial,
              Aggregate(usize),
              Finished
            };

            (0..self.archives.len()).fold(WriteState::Initial, |state, index| {
                match state {
                  WriteState::Initial => {
                      if elapsed as usize >= self.archives[index].retention() {
                          WriteState::Initial
                      } else {
                          self.archives[index].write(&point);
                          WriteState::Aggregate(index)
                      }
                  },

                  WriteState::Aggregate(last_index) => {
                    let (points, timestamp, ratio) = {
                        let seconds_per_point = self.archives[index].seconds_per_point();
                        let ref last_archive = self.archives[last_index];
                        let candidate_point_count = cmp::min((seconds_per_point / last_archive.seconds_per_point()) as usize, last_archive.points());
                        let timestamp = point.0 - (point.0 % seconds_per_point);
                        let from = archive::BucketName(timestamp);
                        let mut candidate_points: Vec<Point> = repeat(Point::default()).take(candidate_point_count).collect();
                        last_archive.read_points(from, &mut candidate_points).unwrap();
                        let points = candidate_points
                            .into_iter()
                            .enumerate()
                            .filter(|&(i, Point(t, _))| timestamp + (i as u32) * last_archive.seconds_per_point() == t)
                            .map(|(_, p)| p)
                            .collect::<Vec<Point>>();
                        let ratio = points.len() as f32 / candidate_point_count as f32;
                        (points, timestamp, ratio)
                    };

                    if ratio >= self.header.x_files_factor() {
                        point.0 = timestamp;
                        point.1 = self.header.aggregation_type().aggregate(&points);
                        self.archives[index].write(&point);
                        WriteState::Aggregate(index)
                    } else {
                        WriteState::Finished
                    }
                  },

                  WriteState::Finished => WriteState::Finished
                }
            });
	}

        #[cfg(test)]
        fn new_transient(schema: &Schema, agg: AggregationType, xff: f32) -> WhisperFile {
            let path = "/dev/null".into();
            let header = Header::new(agg, schema.max_retention(), xff);
            let archives = schema.retention_policies.iter().map(|policy| {
                Archive::new(
                    policy.precision,
                    policy.points() as usize,
                    Mmap::anonymous(policy.size_on_disk() as usize, Protection::ReadWrite).unwrap().into_view_sync()
                )
            }).collect();

            WhisperFile {
                path: path,
                header: header,
                archives: archives
            }
        }

        #[cfg(test)]
        fn into_bytes(self) -> io::Result<Vec<u8>> {
            use whisper::POINT_SIZE;
            let archives_start = Header::archives_start(self.archives.len());
            let mut bytes: Vec<u8> = vec![];
            try!(bytes.write_u32::<BigEndian>(self.header.aggregation_type as u32));
            try!(bytes.write_u32::<BigEndian>(self.header.max_retention() as u32));
            try!(bytes.write_f32::<BigEndian>(self.header.x_files_factor()));
            try!(bytes.write_u32::<BigEndian>(self.archives.len() as u32));
            try!(self.archives.iter().fold(Ok(archives_start), |archive_offset: io::Result<usize>, archive| {
                archive_offset.and_then(|offset| {
                    try!(bytes.write_u32::<BigEndian>(offset as u32));
                    try!(bytes.write_u32::<BigEndian>(archive.seconds_per_point()));
                    try!(bytes.write_u32::<BigEndian>(archive.points() as u32));
                    Ok(offset + archive.points() * POINT_SIZE)
                })
            }));
            for archive in self.archives { bytes.extend_from_slice(archive.slice()); }
            Ok(bytes)
        }
}

#[cfg(test)]
mod tests {
	use whisper::{ Schema, WhisperFile, Point };
	use super::header;

	use std::io::Cursor;
	use std::io::Write;
	use memmap::{ Mmap, Protection };

        /* Sample Empty File
         * Created with:
         *   whisper-create.py <filename> 60:5
	 * Viewed with:
         *   hexdump -v -e '"0x" 1/1 "%02X, "' <filename>
        */
	const SAMPLE_EMPTY_FILE : [u8; 88] = [
		0x00, 0x00, 0x00, 0x01, // agg type = 1 = Average
		0x00, 0x00, 0x01, 0x2C, // max ret
		0x3F, 0x00, 0x00, 0x00, // xff = 0.5
		0x00, 0x00, 0x00, 0x01, // archive count = 1
		0x00, 0x00, 0x00, 0x1C, // a1 offset
		0x00, 0x00, 0x00, 0x3C, // a1 secs/point
		0x00, 0x00, 0x00, 0x05, // a1 points

	// A1: 60 seconds per point, 5 minutes = 5 points
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
	];

        /* Sample File 1
         * Created with:
         *   whisper-create.py --xFilesFactor=0.0 <filename> 1s:10s 10s:1m 1m:3m
         *   whisper-update.py <filename> 1487974954 1
         *   whisper-update.py <filename> 1487974956 3
         *   whisper-update.py <filename> 1487974959 9
         *   whisper-update.py <filename> 1487974962 15
         *   whisper-update.py <filename> 1487974965 62
         *   whisper-update.py <filename> 1487974968 122
         *   whisper-update.py <filename> 1487974970 133
        */
        const SAMPLE_FILE_1: [u8; 280] = [
            0x00, 0x00, 0x00, 0x01, // agg type = 1 = Average
            0x00, 0x00, 0x00, 0xb4, // max ret = 180 = 3 minutes
            0x00, 0x00, 0x00, 0x00, // xff = 0.0
            0x00, 0x00, 0x00, 0x03, // archive count = 3
            0x00, 0x00, 0x00, 0x34, // a1 offset
            0x00, 0x00, 0x00, 0x01, // a1 secs/point
            0x00, 0x00, 0x00, 0x0a, // a1 points
            0x00, 0x00, 0x00, 0xac, // a2 offset
            0x00, 0x00, 0x00, 0x0a, // a2 secs/point
            0x00, 0x00, 0x00, 0x06, // a2 points
            0x00, 0x00, 0x00, 0xf4, // a3 offset
            0x00, 0x00, 0x00, 0x3c, // a3 secs/point
            0x00, 0x00, 0x00, 0x03, // a3 points

            // A1: 1 second per point, 10 seconds = 10 points
            0x58, 0xb0, 0xb2, 0x2a, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x58, 0xb0, 0xb2, 0x35, 0x40, 0x50, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x58, 0xb0, 0xb2, 0x2c, 0x40, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x58, 0xb0, 0xb2, 0x38, 0x40, 0x5e, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x58, 0xb0, 0xb2, 0x2f, 0x40, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x58, 0xb0, 0xb2, 0x3a, 0x40, 0x60, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x58, 0xb0, 0xb2, 0x32, 0x40, 0x2e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

            // A2: 10 seconds per point, 60 seconds = 6 points
            0x58, 0xb0, 0xb2, 0x26, 0x40, 0x11, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x58, 0xb0, 0xb2, 0x30, 0x40, 0x50, 0xd5, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x58, 0xb0, 0xb2, 0x3a, 0x40, 0x60, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

            // A3: 1 minute per point, 3 minutes = 3 points
            0x58, 0xb0, 0xb2, 0x08, 0x40, 0x51, 0x0e, 0x38, 0xe3, 0x8e, 0x38, 0xe3,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        ];

        /* Sample File 2
         * Created with:
         *   whisper-create.py --xFilesFactor=0.33 <filename> 1s:6s 6s:30s 30s:3m
         *   whisper-update.py <filename> 1487981304 0.35
         *   whisper-update.py <filename> 1487981307 0.63
         *   whisper-update.py <filename> 1487981310 0.71
         *   whisper-update.py <filename> 1487981312 0.39
         *   whisper-update.py <filename> 1487981314 0.59
         *   whisper-update.py <filename> 1487981319 0.33
         *   whisper-update.py <filename> 1487981323 0.17
         *   whisper-update.py <filename> 1487981327 0.91
         *   whisper-update.py <filename> 1487981330 0.79
         *   whisper-update.py <filename> 1487981332 0.72
        */
        const SAMPLE_FILE_2: [u8; 256] = [
            0x00, 0x00, 0x00, 0x01, // agg type = 1 = Average
            0x00, 0x00, 0x00, 0xb4, // max ret = 180 = 3 minutes
            0x3e, 0xa8, 0xf5, 0xc3, // xff = 0.33
            0x00, 0x00, 0x00, 0x03, // archive count = 3
            0x00, 0x00, 0x00, 0x34, // a1 offset
            0x00, 0x00, 0x00, 0x01, // a1 secs/point
            0x00, 0x00, 0x00, 0x06, // a1 points
            0x00, 0x00, 0x00, 0x7c, // a2 offset
            0x00, 0x00, 0x00, 0x06, // a2 secs/point
            0x00, 0x00, 0x00, 0x05, // a2 points
            0x00, 0x00, 0x00, 0xb8, // a3 offset
            0x00, 0x00, 0x00, 0x1e, // a3 secs/point
            0x00, 0x00, 0x00, 0x06, // a3 points

            //A1: 1 second per point, 6 seconds = 6 points
            0x58, 0xb0, 0xca, 0xfe, 0x3f, 0xe6, 0xb8, 0x51, 0xeb, 0x85, 0x1e, 0xb8,
            0x58, 0xb0, 0xcb, 0x0b, 0x3f, 0xc5, 0xc2, 0x8f, 0x5c, 0x28, 0xf5, 0xc3,
            0x58, 0xb0, 0xcb, 0x12, 0x3f, 0xe9, 0x47, 0xae, 0x14, 0x7a, 0xe1, 0x48,
            0x58, 0xb0, 0xcb, 0x07, 0x3f, 0xd5, 0x1e, 0xb8, 0x51, 0xeb, 0x85, 0x1f,
            0x58, 0xb0, 0xcb, 0x14, 0x3f, 0xe7, 0x0a, 0x3d, 0x70, 0xa3, 0xd7, 0x0a,
            0x58, 0xb0, 0xcb, 0x0f, 0x3f, 0xed, 0x1e, 0xb8, 0x51, 0xeb, 0x85, 0x1f,

            //A2: 6 seconds per point, 30 seconds = 5 points
            0x58, 0xb0, 0xca, 0xf8, 0x3f, 0xdf, 0x5c, 0x28, 0xf5, 0xc2, 0x8f, 0x5c,
            0x58, 0xb0, 0xca, 0xfe, 0x3f, 0xe2, 0x06, 0xd3, 0xa0, 0x6d, 0x3a, 0x07,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x58, 0xb0, 0xcb, 0x0a, 0x3f, 0xe1, 0x47, 0xae, 0x14, 0x7a, 0xe1, 0x48,
            0x58, 0xb0, 0xcb, 0x10, 0x3f, 0xe8, 0x28, 0xf5, 0xc2, 0x8f, 0x5c, 0x29,

            //A3: 30 seconds per point, 3 minutes = 6 points
            0x58, 0xb0, 0xca, 0xfe, 0x3f, 0xe3, 0xd2, 0x7d, 0x27, 0xd2, 0x7d, 0x28,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        /* Sample File 3
         * Created with:
         *   whisper-create.py --xFilesFactor=0.25 --aggregationMethod=sum <filename> 4s:20s 20s:1m 1m:5m
         *   whisper-update.py <filename> 1487986400 -607.16
         *   whisper-update.py <filename> 1487986405 833.57
         *   whisper-update.py <filename> 1487986411 512.61
         *   whisper-update.py <filename> 1487986416 37.94
         *   whisper-update.py <filename> 1487986420 -315
         *   whisper-update.py <filename> 1487986427 871.87
         *   whisper-update.py <filename> 1487986433 -862.63
         *   whisper-update.py <filename> 1487986439 103.47
         *   whisper-update.py <filename> 1487986443 -10.20
         *   whisper-update.py <filename> 1487986448 366.01
        */
        const SAMPLE_FILE_3: [u8; 208] = [
            0x00, 0x00, 0x00, 0x02, // agg type = 2 = Sum
            0x00, 0x00, 0x01, 0x2c, // max ret = 300 = 5 minutes
            0x3e, 0x80, 0x00, 0x00, // xff = 0.25
            0x00, 0x00, 0x00, 0x03, // archive count = 3
            0x00, 0x00, 0x00, 0x34, // a1 offset
            0x00, 0x00, 0x00, 0x04, // a1 secs/point
            0x00, 0x00, 0x00, 0x05, // a1 points
            0x00, 0x00, 0x00, 0x70, // a2 offset
            0x00, 0x00, 0x00, 0x14, // a2 secs/point
            0x00, 0x00, 0x00, 0x03, // a2 points
            0x00, 0x00, 0x00, 0x94, // a3 offset
            0x00, 0x00, 0x00, 0x3c, // a3 secs/point
            0x00, 0x00, 0x00, 0x05, // a3 points

            // A1: 4 seconds per point, 20 seconds = 5 points
            0x58, 0xb0, 0xdf, 0x08, 0xc0, 0x24, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
            0x58, 0xb0, 0xde, 0xf8, 0x40, 0x8b, 0x3e, 0xf5, 0xc2, 0x8f, 0x5c, 0x29,
            0x58, 0xb0, 0xdf, 0x10, 0x40, 0x76, 0xe0, 0x28, 0xf5, 0xc2, 0x8f, 0x5c,
            0x58, 0xb0, 0xdf, 0x00, 0xc0, 0x8a, 0xf5, 0x0a, 0x3d, 0x70, 0xa3, 0xd7,
            0x58, 0xb0, 0xdf, 0x04, 0x40, 0x59, 0xde, 0x14, 0x7a, 0xe1, 0x47, 0xae,

            // A2: 20 seconds per point, 60 seconds = 3 points
            0x58, 0xb0, 0xde, 0xe0, 0x40, 0x88, 0x47, 0xae, 0x14, 0x7a, 0xe1, 0x48,
            0x58, 0xb0, 0xde, 0xf4, 0xc0, 0x69, 0x49, 0x47, 0xae, 0x14, 0x7a, 0xe1,
            0x58, 0xb0, 0xdf, 0x08, 0x40, 0x76, 0x3c, 0xf5, 0xc2, 0x8f, 0x5c, 0x29,

            // A3: 1 minute per point, 5 minutes = 5 points
            0x58, 0xb0, 0xde, 0xcc, 0x40, 0x81, 0xf5, 0x5c, 0x28, 0xf5, 0xc2, 0x90,
            0x58, 0xb0, 0xdf, 0x08, 0x40, 0x76, 0x3c, 0xf5, 0xc2, 0x8f, 0x5c, 0x29,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        ];

	#[test]
	fn test_header(){
		let mut anon_mmap = Mmap::anonymous(SAMPLE_EMPTY_FILE.len(), Protection::ReadWrite).unwrap();
		{
			let slice : &mut [u8] = unsafe{ anon_mmap.as_mut_slice() };
			let mut cursor = Cursor::new(slice);
			cursor.write(&SAMPLE_EMPTY_FILE[..]).unwrap();
		};

		let hdr = header::Header::new_from_slice(unsafe{ anon_mmap.as_mut_slice() });

		assert_eq!(hdr.aggregation_type(), header::AggregationType::Average);
		assert_eq!(hdr.max_retention(), 300);
		assert_eq!(hdr.x_files_factor(), 0.5);

		let mmap_view = anon_mmap.into_view_sync();
		let archives = hdr.mmap_to_archives(mmap_view);
		assert_eq!(archives.len(), 1);
		assert_eq!(archives[0].seconds_per_point(), 60);
		assert_eq!(archives[0].points(), 5);
		assert_eq!(archives[0].size(), 60); // 5 points * (8 bytes float + 4 bytes ts) = 60 bytes
	}

	#[test]
	fn test_write() {
		let path = "/tmp/blah.wsp";
		let default_specs = vec!["1s:60s".to_string(), "1m:1y".to_string()];
		let schema = Schema::new_from_retention_specs(default_specs).unwrap();

		let mut file = WhisperFile::new(path, &schema, header::AggregationType::Average, 0.50).unwrap();

		file.write(&Point(10, 0.0))
	}

	#[test]
	fn test_aggregation_matches_py() {
            let sample: &[u8] = &SAMPLE_FILE_1;
            let default_specs = vec!["1s:10s".to_string(), "10s:1m".to_string(), "1m:3m".to_string()];
            let schema = Schema::new_from_retention_specs(default_specs).unwrap();
            let mut file = WhisperFile::new_transient(&schema, header::AggregationType::Average, 0.0);
            for &(t, v) in [
              (1487974954, 1.0),
              (1487974956, 3.0),
              (1487974959, 9.0),
              (1487974962, 15.0),
              (1487974965, 65.0),
              (1487974968, 122.0),
              (1487974970, 133.0)
            ].iter() {
              file._write(&Point(t, v), t as i64);
            }
            let result: Vec<u8> = file.into_bytes().unwrap();
            assert_eq!(result, sample);
	}

        #[test]
	fn test_aggregation_matches_py_with_xff() {
            let sample: &[u8] = &SAMPLE_FILE_2;
            let default_specs = vec!["1s:6s".to_string(), "6s:30s".to_string(), "30s:3m".to_string()];
            let schema = Schema::new_from_retention_specs(default_specs).unwrap();
            let mut file = WhisperFile::new_transient(&schema, header::AggregationType::Average, 0.33);
            for &(t, v) in [
                (1487981304, 0.35),
                (1487981307, 0.63),
                (1487981310, 0.71),
                (1487981312, 0.39),
                (1487981314, 0.59),
                (1487981319, 0.33),
                (1487981323, 0.17),
                (1487981327, 0.91),
                (1487981330, 0.79),
                (1487981332, 0.72),
            ].iter() {
                file._write(&Point(t, v), t as i64);
            }
            let result: Vec<u8> = file.into_bytes().unwrap();
            assert_eq!(result, sample);
        }

        #[test]
	fn test_aggregation_matches_py_with_sum() {
            let sample: &[u8] = &SAMPLE_FILE_3;
            let default_specs = vec!["4s:20s".to_string(), "20s:60s".to_string(), "1m:5m".to_string()];
            let schema = Schema::new_from_retention_specs(default_specs).unwrap();
            let mut file = WhisperFile::new_transient(&schema, header::AggregationType::Sum, 0.25);
            for &(t, v) in [
                (1487986400, -607.16),
                (1487986405, 833.57),
                (1487986411, 512.61),
                (1487986416, 37.94),
                (1487986420, -315.0),
                (1487986427, 871.87),
                (1487986433, -862.63),
                (1487986439, 103.47),
                (1487986443, -10.20),
                (1487986448, 366.01),
            ].iter() {
                file._write(&Point(t, v), t as i64);
            }
            let result: Vec<u8> = file.into_bytes().unwrap();
            assert_eq!(result, sample);
        }
}
