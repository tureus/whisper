use super::file::archive::BucketName;

use std::io::Cursor;

use byteorder::{ ByteOrder, BigEndian, WriteBytesExt };

pub const POINT_SIZE : usize = 12;

#[derive(Debug,PartialEq)]
pub struct Point(pub u32, pub f64);

impl Point {
    #[inline]
    pub fn new_from_slice(slice: &[u8]) -> Point {
        let ts = BigEndian::read_u32(&slice[0..4]);
        let val = BigEndian::read_f64(&slice[4..]);
        Point(ts,val)
    }

    #[inline]
    pub fn write_to_slice(&self, bucket_name: BucketName, slice: &mut [u8]) {
        let mut writer = Cursor::new(slice);
        writer.write_u32::<BigEndian>(bucket_name.0).unwrap();
        writer.write_f64::<BigEndian>(self.1).unwrap();
    }
}
