use std::path::PathBuf;
use std::str;

use whisper::Point;

#[derive(PartialEq,Debug)]
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

    pub fn from_datagram(datagram_buffer: &[u8]) -> Result<NamedPoint, String > {
        let datagram = match str::from_utf8(datagram_buffer) {
            Ok(body) => body,
            Err(_) => return Err( "invalid utf8 character".to_string() )
        };

        // TODO: this seems too complicated just to detect/remove "\n"
        // And it scans the string as utf8 codepoints.
        // Will this just be ASCII... is it safe to skip utf8-ness? (probs not)
        let (without_newline,newline_str) = {
        	datagram.split_at(datagram.len()-1)

        	// old code (terribad?)
            // let len = datagram.len();
            // ( 
            //   datagram.slice_chars(0, len-1),
            //   datagram.slice_chars(len-1, len)
            // )
        };
        if newline_str != "\n" {
            return Err( format!("Datagram `{}` is missing a newline `{}`", datagram, newline_str) )
        }

        let parts : Vec<&str> = without_newline.split(" ").collect();
        if parts.len() != 3 {
            return Err( format!("Datagram `{}` does not have 3 parts", datagram) );
        }

        // TODO: copies to msg. Used to be a reference from datagram_buffer
        // but figuring out how to keep the datagram_buffer (which was on the heap)
        // alive long enough was tricky.
        let metric_name = parts[0].to_string();

        let value = {
            let value_parse = parts[1].parse::<f64>();
            match value_parse {
                Ok(val) => val,
                Err(_) => {
                    return Err( format!("Datagram value `{}` is not a float", parts[1]) )
                }
            }
        };

        let timestamp = {
            let timestamp_parse = parts[2].parse::<u32>();
            match timestamp_parse {
                Ok(val) => val,
                Err(_) => {
                    return Err( format!("Datagram value `{}` is not an unsigned integer", parts[2]) )
                }
            }
        };

        let msg = NamedPoint::new(metric_name, timestamp, value);
        Ok(msg)

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

#[cfg(test)]
mod tests {
    extern crate test;
    use self::test::Bencher;

    use whisper::Point;
    use super::*;
    use std::path::Path;

    #[bench]
    fn bench_good_datagram(b: &mut Bencher){
        let datagram = "home.pets.bears.lua.purr_volume 100.00 1434598525\n";

        b.iter(|| {
            let msg_opt = NamedPoint::from_datagram(datagram.as_bytes());
            msg_opt.unwrap();
        });
    }

    #[test]
    fn test_good_datagram() {
        let datagram = "home.pets.bears.lua.purr_volume 100.00 1434598525\n";
        let msg_opt = NamedPoint::from_datagram(datagram.as_bytes());
        let msg = msg_opt.unwrap();

        let expected = NamedPoint {
            metric_name: "home.pets.bears.lua.purr_volume".to_string(),
            point: Point(1434598525, 100.0)
        };

        assert_eq!(msg, expected);
    }

    #[bench]
    fn bench_bad_datagram(b: &mut Bencher){
        let datagram = "home.pets.monkeys.squeeky.squeeks asdf 1434598525\n";

        b.iter(|| {
            let msg_opt = NamedPoint::from_datagram(datagram.as_bytes());
            assert!(msg_opt.is_err());
        });
    }
}
