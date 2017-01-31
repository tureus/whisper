use std::path::PathBuf;

error_chain! {
  foreign_links {
    Io(::std::io::Error);
  }
}
