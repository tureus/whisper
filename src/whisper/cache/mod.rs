// use carbon::CarbonMsg;
// use whisper::{ WhisperFile, MutexWhisperFile };
use whisper::{ WhisperFile, Schema };
use std::collections::HashMap;
use std::path::{ Path, PathBuf };
use std::fs::{ PathExt, DirBuilder };
use std::io;

mod named_point;
pub use self::named_point::NamedPoint;

pub struct WhisperCache {
	pub base_path: PathBuf,
	open_files: HashMap< PathBuf, WhisperFile >,
	schema: Schema
}

impl WhisperCache {
	pub fn new(base_path: &Path, schema: Schema) -> WhisperCache {
		WhisperCache {
			base_path: base_path.to_path_buf(),
			open_files: HashMap::new(),
			schema: schema
		}
	}

	pub fn write(&mut self, named_point: NamedPoint) -> Result<(), io::Error> {
		let metric_rel_path = named_point.rel_path();

		let mut whisper_file = try!( self.open(metric_rel_path) );

		// We assume opened files always succeed in writes
		whisper_file.write(&named_point.point());
		Ok(())
	}

	fn open(&mut self, metric_rel_path: PathBuf) -> Result< &mut WhisperFile, io::Error> {

		if self.open_files.contains_key(&metric_rel_path) {

			// debug!("file cache hit. resolved {:?}", metric_rel_path);
			Ok( self.open_files.get_mut(&metric_rel_path).unwrap() )

		} else {

			// debug!("file cache miss. resolving {:?}", metric_rel_path);

			let path_for_insert = metric_rel_path.clone();
			let path_for_relookup = metric_rel_path.clone();

			let path_on_disk = self.base_path.join(metric_rel_path);

			let whisper_file = if path_on_disk.exists() && path_on_disk.is_file() {

				// debug!("`{:?}` exists on disk. opening.", path_on_disk);
				WhisperFile::open(&path_on_disk)

			} else {

				// Verify the folder structure is present.
				// TODO: benchmark (for my own curiosity)
				// TODO: assumption here is that we do not store in root FS
				if !path_on_disk.parent().unwrap().is_dir() {
					// debug!("`{:?}` must be created first", path_on_disk.parent());
					try!( DirBuilder::new().recursive(true).create( path_on_disk.parent().unwrap() ) );
				}
				try!( WhisperFile::new(&path_on_disk, &self.schema) )

			};

			self.open_files.insert(path_for_insert, whisper_file );
			Ok( self.open_files.get_mut(&path_for_relookup).unwrap() )

		}

	}
}

#[cfg(test)]
mod test {
	extern crate test;
	use test::Bencher;

	use std::path::{ Path };

	use whisper::{ WhisperCache, NamedPoint, Schema, Point };

	#[bench]
	fn test_opening_new_whisper_file(b: &mut Bencher){
		let default_specs = vec!["1s:60s".to_string(), "1m:1y".to_string()];
		let schema = Schema::new_from_retention_specs(default_specs);

		let mut cache = WhisperCache::new(&Path::new("/tmp"), schema);
		let current_time = 1434598525;

		b.iter(move ||{
			let metric = NamedPoint::new("hey.there.bear".to_string(), 1434598525, 0.0);
			cache.write(metric).unwrap();
		});
	}
}
