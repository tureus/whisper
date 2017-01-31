error_chain! {
  errors {
    InvalidRetentionPolicy(reason: String) {
      description("Invalid retention policy")
      display("Invalid retention policy: {}", reason)
    }
  }
}
