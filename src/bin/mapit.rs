extern crate mmap;
extern crate whisper;

use std::path::Path;

use whisper::{ WhisperFile, Point };

fn main(){
	let base_path = Path::new("test/whisper/");
	let mut whisps = vec![];

	let path = base_path.join("60s_1y.wsp").to_path_buf();
	let mut whisp = WhisperFile::open(&path);
	whisp.write( Point(100, 0.0) );
	// whisp.write()

	whisps.push(whisp);
}