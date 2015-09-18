extern crate whisper;

use std::path::Path;

use whisper::{ WhisperFile, Point };

fn main(){
	let base_path = Path::new("test/whisper/");

	let path = base_path.join("60s_1y.wsp").to_path_buf();
	let whisp = WhisperFile::open(&path);
	println!("{:?}", whisp);
}
