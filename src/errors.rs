
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {

    #[error("StorageNodeError: {0}")]
    StorageNodeError(String),

    #[error("CorruptData: {0}")]
    CorruptedData(String),

    #[error("IO: {0}")]
    IO(String),

    #[error("Other: {0}")]
    Other(String),
}

impl  From<Error> for String {
    fn from(e: Error) -> String {
        format!("{:?}", e)
    }
}