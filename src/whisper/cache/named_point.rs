use std::path::PathBuf;

use whisper::Point;

pub struct NamedPoint {
	metric_name: String,
	point: Point
}

impl NamedPoint {
	pub fn new(name: String, timestamp: u32, value: f64) -> NamedPoint {
		NamedPoint {
			metric_name: name,
			point: Point(timestamp, value)
		}
	}

	pub fn rel_path(&self) -> PathBuf {
        // Would love to have the NamedPoint keep the UDP datagram or whatever around.
        // But easier to copy that string to this `metric_name` and carry on!
        let mut rel_path : String = self.metric_name.replace(".","/");
        rel_path.push_str(".wsp");
        PathBuf::from(rel_path)
	}

	pub fn point(&self) -> &Point {
		&self.point
	}
}
