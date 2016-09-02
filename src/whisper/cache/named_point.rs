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

    pub fn from_datagram(datagram_buffer: &[u8]) -> Result< Vec<NamedPoint>, String > {
        let datagram = match str::from_utf8(datagram_buffer) {
            Ok(body) => body,
            Err(_) => return Err( "invalid utf8 character".to_string() )
        };

        let parsed_lines : Vec<Result<NamedPoint,String>> = datagram.lines().map(|x| NamedPoint::parse_line(x) ).collect();
        if parsed_lines.iter().any(|x| x.is_err() ) {
        	Err("datagram had invalid entries. skipping all.".to_string())
        } else {
        	Ok(parsed_lines.into_iter().map(|x| x.unwrap()).collect())
        }

    }

    pub fn parse_line(line: &str) -> Result< NamedPoint, String > {
        let parts : Vec<&str> = line.split(" ").collect();
        if parts.len() != 3 {
            return Err( format!("Datagram `{}` does not have 3 parts", line) );
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
                		0.0
                    // return Err( format!("Datagram value `{}` is not a float", parts[1]) )
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

    #[bench]
    fn bench_good_datagram_line(b: &mut Bencher){
        let datagram = "home.pets.bears.lua.purr_volume 100.00 1434598525";

        b.iter(|| {
            let msg_opt = NamedPoint::parse_line(datagram);
            msg_opt.unwrap();
        });
    }

    #[test]
    fn test_good_datagram_line() {
        let datagram = "home.pets.bears.lua.purr_volume 100.00 1434598525";
        let msg_opt = NamedPoint::parse_line(datagram);
        let msg = msg_opt.unwrap();

        let expected = NamedPoint {
            metric_name: "home.pets.bears.lua.purr_volume".to_string(),
            point: Point(1434598525, 100.0)
        };

        assert_eq!(msg, expected);
    }

    #[test]
    fn test_long_datagram() {
    	let datagram = "collectd.xle.xle-forwarder-01.disk-vda.disk_octets.read nan 1442949342\r\ncollectd.xle.xle-forwarder-01.disk-vda.disk_octets.write nan 1442949342\r\n";
    	let msgs_opt = NamedPoint::from_datagram(datagram.as_bytes());
    	assert!(msgs_opt.is_ok());

    	let expected = vec![
    		NamedPoint {
    			metric_name: "collectd.xle.xle-forwarder-01.disk-vda.disk_octets.read".to_string(),
    			point: Point(1442949342, 0.0)
    		},
    		NamedPoint {
    			metric_name: "collectd.xle.xle-forwarder-01.disk-vda.disk_octets.write".to_string(),
    			point: Point(1442949342, 0.0)
    		}
    	];
    	assert_eq!(msgs_opt.unwrap(), expected);
    }

    #[bench]
    fn bench_bad_datagram(b: &mut Bencher){
        let datagram = "home.pets.monkeys.squeeky.squeeks nan";

        b.iter(|| {
            let msg_opt = NamedPoint::from_datagram(datagram.as_bytes());
            assert!(msg_opt.is_err());
        });
    }
}
