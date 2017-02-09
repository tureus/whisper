mod retention_policy;

use whisper::file::STATIC_HEADER_SIZE;
use whisper::file::ARCHIVE_INFO_SIZE;
use whisper::errors::Result;
pub use self::retention_policy::RetentionPolicy;

#[derive(Debug)]
pub struct Schema {
    pub retention_policies: Vec<RetentionPolicy>
}

impl Schema {
    pub fn new_from_retention_specs(specs: Vec<String>) -> Result<Schema> {
        let retention_policies: Result<Vec<RetentionPolicy>> =
            specs.iter().fold(Ok(vec![]), |policies_result, next| {
                policies_result
                    .and_then(|mut policies| RetentionPolicy::spec_to_retention_policy(next)
                        .map(|policy| { policies.push(policy); policies })
                    )
            });

        retention_policies.map(|policies| Schema { retention_policies: policies })
    }

    pub fn header_size_on_disk(&self) -> u32 {
        STATIC_HEADER_SIZE as u32 +
        (ARCHIVE_INFO_SIZE*self.retention_policies.len()) as u32
    }

    pub fn size_on_disk(&self) -> u32 {
        let retentions_disk_size = self.retention_policies.iter().fold(0, |tally, policy| {
            // debug!("policy: {:?} size on disk: {}", policy, policy.size_on_disk());
            tally + policy.size_on_disk()
        });

        self.header_size_on_disk() + retentions_disk_size
    }

    pub fn max_retention(&self) -> u32 {
        if self.retention_policies.len() == 0 {
            0
        } else {
            self.retention_policies.iter().map(|&rp| rp.retention).max().unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use whisper::file::{ STATIC_HEADER_SIZE, ARCHIVE_INFO_SIZE };

    #[test]
    fn test_size_on_disk(){
        let first_policy = RetentionPolicy {
            precision: 1,
            retention: 60
        };

        let second_policy = RetentionPolicy {
            precision: 60,
            retention: 60
        };


        let mut little_schema = Schema {
            retention_policies: vec![]
        };

        let expected = STATIC_HEADER_SIZE as u32
                + ARCHIVE_INFO_SIZE as u32 * 2
                + 60*12 // first policy size
                + 1*12; // second policy size

        little_schema.retention_policies.push(first_policy);
        little_schema.retention_policies.push(second_policy);

        assert_eq!(little_schema.size_on_disk(), expected);
    }

}
