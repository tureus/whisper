// use carbon::CarbonMsg;
// use whisper::{ WhisperFile, MutexWhisperFile };
use whisper::{ WhisperFile, Schema };
use std::path::{ Path, PathBuf };
use std::fs::DirBuilder;
use std::io;
use std::sync::{ Arc, Mutex };
use lru_cache::LruCache;
use whisper::errors::file::{Error, ErrorKind, Result, ResultExt};

mod named_point;
pub use self::named_point::NamedPoint;

type WhisperMutex = Arc<Mutex<WhisperFile>>;

pub struct WhisperCache {
	pub base_path: PathBuf,
	// open_files: HashMap< PathBuf, WhisperMutex >,
	open_files: LruCache< PathBuf, WhisperMutex >,
	schema: Schema
}

impl WhisperCache {
	pub fn new<P>(base_path: P, size: usize, schema: Schema) -> WhisperCache
        where P: AsRef<Path> {
		WhisperCache {
			base_path: base_path.as_ref().to_path_buf(),
			open_files: LruCache::new(size),
			schema: schema
		}
	}

	pub fn write(&mut self, named_point: NamedPoint) -> Result<()> {
		let metric_rel_path = named_point.rel_path();
		self.get(&metric_rel_path).map(|cache_entry| {
                        let mut whisper_file = cache_entry.lock().unwrap();

                        // We assume opened files always succeed in writes
                        whisper_file.write(&named_point.point());
                })
	}

	fn get(&mut self, metric_rel_path: &PathBuf) -> Result<&WhisperMutex> {
		if self.open_files.contains_key(metric_rel_path) {
			debug!("file cache hit. resolved {:?}", metric_rel_path);
			Ok(self.open_files.get_mut(metric_rel_path).unwrap())
		} else {
			// debug!("file cache miss. resolving {:?}", metric_rel_path);
			let path_in_cache = metric_rel_path;
			let path_on_disk = self.base_path.join(metric_rel_path);
			let whisper_file = if path_on_disk.exists() && path_on_disk.is_file() {
				debug!("`{:?}` exists on disk. opening.", path_on_disk);
				try!(WhisperFile::open(&path_on_disk))
			} else {

				// Verify the folder structure is present.
				// TODO: benchmark (for my own curiosity)
				// TODO: assumption here is that we do not store in root FS
				if !path_on_disk.parent().unwrap().is_dir() {
					debug!("parent dir for `{:?}` must be created first", path_on_disk.parent());
					try!( DirBuilder::new().recursive(true).create( path_on_disk.parent().unwrap() ) );
				}
				debug!("`{:?}` must now be created", path_on_disk);
				try!(WhisperFile::new(&path_on_disk, &self.schema))

			};

			self.open_files.insert(path_in_cache.clone(), Arc::new(Mutex::new(whisper_file)));
			Ok(self.open_files.get_mut(path_in_cache).unwrap())
		}
	}
}

#[cfg(test)]
mod test {
	extern crate test;
	use test::Bencher;
	use whisper::{ WhisperCache, NamedPoint, Schema };

	#[bench]
	fn test_opening_new_whisper_file(b: &mut Bencher){
		let default_specs = vec!["1s:60s".to_string(), "1m:1y".to_string()];
		let schema = Schema::new_from_retention_specs(default_specs).unwrap();

		let mut cache = WhisperCache::new("/tmp", 100, schema);
		let current_time = 1434598525;

		b.iter(move ||{
			let metric = NamedPoint::new("hey.there.bear".to_string(), current_time, 0.0);
			cache.write(metric).unwrap();
		});
	}
}
