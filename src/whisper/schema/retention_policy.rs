use whisper::point::POINT_SIZE;
use whisper::file::archive::ARCHIVE_INFO_SIZE;
use whisper::errors::{SchemaError, Result};

use byteorder::{ BigEndian, WriteBytesExt };
use regex;

use std::io::{ BufWriter, Write };
use std::fs::File;

// A RetentionPolicy is the abstract form of an ArchiveInfo
// It does not know it's position in the file. Should it just
// be collapsed in to ArchiveInfo? Possibly.
#[derive(Debug, Clone, Copy)]
pub struct RetentionPolicy {
    pub precision: u32,
    pub retention: u32
}

impl RetentionPolicy {
    pub fn spec_to_retention_policy(spec: &str) -> Result<RetentionPolicy> {
        // TODO: regex should be built as const using macro regex!
        // but that's only available in nightlies.
        let retention_matcher = regex::Regex::new({r"^(\d+)([smhdwy])?:(\d+)([smhdwy])?$"}).unwrap();
        match retention_matcher.captures(spec) {
            Some(regex_match) => retention_capture_to_pair(spec, regex_match),
            None => Err(SchemaError(format!("Policy '{}' is in an invalid format", spec)))
        }
    }

    // TODO how do we guarantee even divisibility?
    pub fn points(&self) -> u32 {
        self.retention / self.precision
    }

    pub fn size_on_disk(&self) -> u32 {
        self.points() * POINT_SIZE as u32
    }

    pub fn write(&self, mut file: &File, offset: u64) {
        // debug!("writing retention policy (offset: {})", offset);
        let mut arr = [0u8; ARCHIVE_INFO_SIZE as usize];
        let buf : &mut [u8] = &mut arr;

        self.fill_buf(buf, offset);
        file.write_all(buf).unwrap();
    }

    pub fn fill_buf(&self, buf: &mut [u8], offset: u64) {
        let mut writer = BufWriter::new(buf);
        let points = self.retention / self.precision;

        writer.write_u32::<BigEndian>( offset as u32 ).unwrap();
        writer.write_u32::<BigEndian>( self.precision as u32 ).unwrap();
        writer.write_u32::<BigEndian>( points as u32 ).unwrap();
    }
}

fn retention_capture_to_pair(original_spec: &str, regex_match: regex::Captures) -> Result<RetentionPolicy> {
    let precision_opt = regex_match.get(1).map(|m| m.as_str());
    let precision_mult = regex_match.get(2).map(|m| m.as_str()).unwrap_or("s");
    let retention_opt = regex_match.get(3).map(|m| m.as_str());
    let retention_mult = regex_match.get(4).map(|m| m.as_str());

    match (precision_opt, retention_opt) {
        (Some(precision), Some(retention)) => {
            precision.parse::<u32>()
                .map_err(|e| SchemaError(format!("Unable to parse precision '{}' in policy '{}' as u32\nCaused by: {}", precision, original_spec, e)))
                .and_then(|base_precision| mult_str_to_num(precision_mult).map(|mult| base_precision * mult))
                .and_then(|precision| {
                    retention.parse::<u32>()
                        .map_err(|e| SchemaError(format!("Unable to parse retention '{}' in policy '{}' as u32\nCaused by: {}", retention, original_spec, e)))
                        .and_then(|base_retention| match retention_mult {
                            Some(mult_str) => {
                                mult_str_to_num(mult_str).map(|mult| base_retention * mult)
                            },
                            None => {
                                // user has not provided a multipler so this is interpreted
                                // as the number of points so we have to
                                // calculate retention from the number of points
                                Ok(base_retention * precision)
                            }
                        }).map(|retention| RetentionPolicy {
                            precision: precision,
                            retention: retention
                        })
            })
        },
        (precision, retention) => {
            let precision_msg = precision.map(|p| format!("Precision is present ('{}')", p)).unwrap_or_else(|| "Precision is absent".to_string());
            let retention_msg = retention.map(|r| format!("Retention is present ('{}')", r)).unwrap_or_else(|| "Retention is absent".to_string());
            Err(SchemaError(
                format!("Both precision and retention values must be present in policy '{}':\n{}\n{}",
                    original_spec, precision_msg, retention_msg)
            ))
        }
    }
}

fn mult_str_to_num(mult_str: &str) -> Result<u32> {
    // TODO: is this exactly how whisper does it?
    match mult_str {
        "s" => Ok(1),
        "m" => Ok(60),
        "h" => Ok(60*60),
        "d" => Ok(60*60*24),
        "w" => Ok(60*60*24*7),
        "y" => Ok(60*60*24*365),
        //Regex should ensure this is impossible
        _ => Err(SchemaError(format!("Unrecognized time multiplier specified: '{}'", mult_str)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use whisper::point::POINT_SIZE;

    #[test]
    fn test_size_on_disk(){
        let five_minute_retention = RetentionPolicy {
            precision: 60, // 1 sample/minute
            retention: 5*60 // 5 minutes
        };

        let expected = five_minute_retention.size_on_disk();
        assert_eq!(expected, 5*POINT_SIZE as u32);
    }

    #[test]
    fn test_spec_without_multipliers() {
        let spec = "15:60";
        let expected = RetentionPolicy {
            precision: 15,
            retention: 15*60
        };

        let retention_opt = RetentionPolicy::spec_to_retention_policy(spec);
        assert!(retention_opt.is_ok());
        let retention_policy = retention_opt.unwrap();
        assert_eq!(retention_policy.precision, expected.precision);
        assert_eq!(retention_policy.retention, expected.retention);
    }

    #[test]
    fn test_spec_with_multipliers() {
        let spec = "1d:60y";
        let expected = RetentionPolicy {
            precision: 1 *  60*60*24,
            retention: 60 * 60*60*24*365
        };

        let retention_opt = RetentionPolicy::spec_to_retention_policy(spec);
        assert!(retention_opt.is_ok());
        let retention_policy = retention_opt.unwrap();
        assert_eq!(retention_policy.precision, expected.precision);
        assert_eq!(retention_policy.retention, expected.retention);
    }

    #[test]
    fn test_invalid_empty_spec() {
        let spec = "";
        let retention_opt = RetentionPolicy::spec_to_retention_policy(spec);
        let expected = format!("Error: Invalid schema: Policy '{}' is in an invalid format\n", spec);
        assert_eq!(format!("{}", retention_opt.unwrap_err()), expected)
    }

    #[test]
    fn test_invalid_precision_spec() {
        let spec = "1x:60y";
        let retention_opt = RetentionPolicy::spec_to_retention_policy(spec);
        let expected = format!("Error: Invalid schema: Policy '{}' is in an invalid format\n", spec);
        assert_eq!(format!("{}", retention_opt.unwrap_err()), expected)
    }

    #[test]
    fn test_invalid_retention_spec() {
        let spec = "15:60e";
        let retention_opt = RetentionPolicy::spec_to_retention_policy(spec);
        let expected = format!("Error: Invalid schema: Policy '{}' is in an invalid format\n", spec);
        assert_eq!(format!("{}", retention_opt.unwrap_err()), expected)
    }

    #[test]
    fn test_overflow_precision_amount() {
        let precision = ::std::u32::MAX as u64 + 1;
        let spec = format!("{}s:60y", precision.to_string());
        let retention_opt = RetentionPolicy::spec_to_retention_policy(&spec);
        let expected = format!("Error: Invalid schema: Unable to parse precision '{}' in policy '{}' as u32\nCaused by: number too large to fit in target type\n", precision, spec);
        assert_eq!(format!("{}", retention_opt.unwrap_err()), expected)
    }

    #[test]
    fn test_overflow_retention_amount() {
        let retention = ::std::u32::MAX as u64 + 7;
        let spec = format!("30s:{}y", retention.to_string());
        let retention_opt = RetentionPolicy::spec_to_retention_policy(&spec);
        let expected = format!("Error: Invalid schema: Unable to parse retention '{}' in policy '{}' as u32\nCaused by: number too large to fit in target type\n", retention, spec);
        assert_eq!(format!("{}", retention_opt.unwrap_err()), expected)
    }

    #[test]
    fn test_missing_precision_amount() {
        let spec = "15s";
        let retention_opt = RetentionPolicy::spec_to_retention_policy(spec);
        let expected = format!("Error: Invalid schema: Policy '{}' is in an invalid format\n", spec);
        assert_eq!(format!("{}", retention_opt.unwrap_err()), expected)
    }
}
