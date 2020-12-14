use crate::transport::Compression;

/// Custom connection parameters
#[derive(Copy, Clone, Debug)]
pub struct ConnectionParams {
    pub compression: Option<Compression>,
    pub nodelay: bool,
}

impl Default for ConnectionParams {
    fn default() -> Self {
        Self {
            compression: None,
            nodelay: true,
        }
    }
}
