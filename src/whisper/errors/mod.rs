use std::error::Error;
use std::fmt::{self, Display};

#[derive(Debug, Clone)]
pub struct SchemaError(pub String);
pub type Result<T> = ::std::result::Result<T, SchemaError>;

impl Display for SchemaError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    let &SchemaError(ref reason) = self;
    write!(f, "Error: {}: {}\n", self.description(), reason)
  }
}

impl Error for SchemaError {
  fn description(&self) -> &str {
    "Invalid schema"
  }
}

