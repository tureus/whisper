extern crate mmap;
extern crate whisper_mmap;

use std::path::Path;

use whisper_mmap::WhisperFile;

// fn block(){
// 	let mut input = String::new();
// 	match std::io::stdin().read_line(&mut input) {
// 	   _ => {}
// 	}
// }

fn main(){
	let base_path = Path::new("test/whisper/");
	let mut whisps = vec![];

	let path = base_path.join("60s_1y.wsp").to_path_buf();
	let whisp = WhisperFile::open(path);
	
	whisps.push(whisp);
}