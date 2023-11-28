use std::thread::JoinHandle;

use crossbeam_channel::Receiver;

use crate::{errors::Error, datum::RowData};

use super::FileWriteWrap;

pub trait TiDBExporter {
    fn start_export(&mut self, rx : Receiver<Vec<Box<RowData>>>) -> Vec<JoinHandle<()>>;


    //have to set thread num before start_export
    fn set_thread_num(&mut self, num : usize);

    fn set_debug_mode(&mut self, is_debug : bool);
}

pub trait TiDBFileExporter {
    fn create_file_write_wrap(write_path : &str, maximum_file_size_mb : usize, is_gzip : bool) -> Result<FileWriteWrap, Error> {
        let file_size = maximum_file_size_mb * 1024 * 1024;
        let fw = FileWriteWrap::new(write_path, file_size, is_gzip)?;
        Ok(fw)
    }
}