use byteorder::{ByteOrder, BigEndian };

pub const POINT_SIZE_ON_DISK : usize = 12;

#[derive(Debug,PartialEq)]
pub struct Point(pub u32, pub f64);

impl Point {
	pub fn new_from_slice(slice: &[u8]) -> Point {

    	let ts = BigEndian::read_u32(&slice[0..4]);
    	let val = BigEndian::read_f64(&slice[4..]);
    	Point(ts,val)
	}
}