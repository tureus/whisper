#[macro_use]
extern crate log;
extern crate env_logger;
extern crate rustc_serialize;
extern crate docopt;
extern crate time;

extern crate whisper;

use docopt::Docopt;
use whisper::{WhisperFile, Point, Schema, AggregationType};
use std::path::Path;

static USAGE: &'static str = "
Whisper is the fast file manipulator

Usage:
    whisper info <file>
    whisper dump <file>
    whisper update <file> <timestamp> <value>
    whisper mark <file> <value>
    whisper thrash <file> <value> <times>
    whisper create <file> <timespec>...

Options:
    --xff <x_files_factor>
    --aggregation_method <method>
";

#[derive(RustcDecodable, Debug)]
struct Args {
    cmd_info: bool,
    cmd_dump: bool,
    cmd_update: bool,
    cmd_mark: bool,
    cmd_thrash: bool,
    cmd_create: bool,

    arg_file: String,
    arg_timestamp: String,
    arg_value: String,
    arg_times: String,

    arg_timespec: Vec<String>
}


pub fn main(){
    env_logger::init().unwrap();
    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.decode())
                            .unwrap_or_else(|e| e.exit());

    let arg_file = args.arg_file.clone();
    let path: &str = unsafe {
        arg_file.slice_unchecked(0, args.arg_file.len())
    };

    let current_time = time::get_time().sec as u64;

    if args.cmd_info {
        cmd_info(path);
    } else if args.cmd_dump {
        cmd_dump(path);
    } else if args.cmd_update {
        cmd_update(args, path, current_time);
    } else if args.cmd_mark {
        cmd_mark(args, path, current_time);
    } else if args.cmd_thrash {
        cmd_thrash(args, path, current_time);
    } else if args.cmd_create {
        cmd_create(args, path);
    } else {
        println!("Must specify command.");
    }
}

fn cmd_info<P>(path: P)
  where P: AsRef<Path> {
    let whisper_file = WhisperFile::open(path);
    // TODO: used to simpler of Display, not Debug
    println!("{:?}", whisper_file);
}

fn cmd_dump<P>(path: P)
  where P: AsRef<Path> {
    let whisper_file = WhisperFile::open(path);
    println!("{:?}", whisper_file);
}

#[allow(unused_variables)] /*TODO: Remove once we reenable writing current_time*/
fn cmd_update<P>(args: Args, path: P, current_time: u64)
  where P: AsRef<Path> {
    WhisperFile::open(path).map(|mut file| {
      let point = Point(args.arg_timestamp.parse::<u32>().unwrap(),
                        args.arg_value.parse::<f64>().unwrap());
      debug!("Updating TS: {} with value: {}", point.0, point.1);

      file.write(/*current_time, TODO: reenable */ &point);
    }).unwrap_or_else(|e| println!("Unable to open whisper file: {}", e))
}

fn cmd_mark<P>(args: Args, path: P, current_time: u64)
  where P: AsRef<Path> {
    WhisperFile::open(path).map(|mut file| {
      let point = Point(current_time as u32, args.arg_value.parse::<f64>().unwrap());

      file.write(/*current_time, TODO: reenable */ &point);
    }).unwrap_or_else(|e| println!("Unable to open whisper file: {}", e))
}

fn cmd_thrash<P>(args: Args, path: P, current_time: u64)
  where P: AsRef<Path> {
    let times = args.arg_times.parse::<u32>().unwrap();
    WhisperFile::open(path).map(|mut file| {
      for index in 1..times {
          let point = Point(current_time as u32+index,
                            args.arg_value.parse::<f64>().unwrap());

          file.write(&point);
      }
    }).unwrap_or_else(|e| println!("Unable to open whisper file: {}", e))
}

fn cmd_create<P>(args: Args, path: P)
  where P: AsRef<Path> {
    let schema = Schema::new_from_retention_specs(args.arg_timespec).unwrap();
    let new_result = WhisperFile::new(path, &schema, AggregationType::Average, 0.5);
    match new_result {
    	// TODO change to Display
        Ok(whisper_file) => println!("Success! {:?}", whisper_file),
        Err(why) => println!("Failed: {:?}", why)
    }
}
