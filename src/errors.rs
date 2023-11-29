
use thiserror::Error;
use hex;

#[derive(Debug, Error)]
pub enum Error {

    #[error("StorageNodeError: {0}")]
    StorageNodeError(String),

    #[error("CorruptedData: {0}")]
    CorruptedData(String),

    #[error("CorruptDataBytes: {0}")]
    CorruptedDataBytes(String, Box<[u8]>),

    #[error("CorruptDataString: {0}")]
    CorruptedDataString(String, String),

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

pub fn display_corrupted_err_data(err : &Error) {
    if let Error::CorruptedDataBytes(_, data) = err {
        display_bytes_err_data(&data);
    } else if let Error::CorruptedDataString(_, data) = err {
        display_string_err_data(&data);
    }

    return;
}

pub fn display_bytes_err_data(data : &[u8]) {
    let encoded_str = hex::encode(data);
    print!("\n********Error Data********\n{}\n********Error Data End********\n",encoded_str);
}

pub fn display_string_err_data(str : &str) {
    print!("\n********Error Data********\n{}\n********Error Data End********\n",str);
}