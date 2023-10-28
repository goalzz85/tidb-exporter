
use std::{io::Write, path::{Path, PathBuf}};

use crate::datum::RowData;

use super::errors::Error;

pub mod csvwriter;

pub trait TiDBExportWriter {
    fn write_row_data(&mut self, row_data : Box<RowData>) -> Result<(), Error>;
    fn check_split_file(&mut self) -> Result<(), Error>;
    fn flush(&mut self);
}

pub struct WriteWrap {
    file_size : usize,
    cur_file_writed_size : usize,
    cur_write : Box<dyn std::io::Write>,
    is_need_split : bool,
}

impl WriteWrap {
    pub fn new(export_path : &str, file_size : usize, file_num : i32, is_gzip : bool) -> Result<WriteWrap, Error> {
        let w = Self::get_write(export_path, file_num, is_gzip)?;

        return Ok(WriteWrap {
            file_size : file_size,
            cur_file_writed_size : 0,
            cur_write : w,
            is_need_split : false,
        });
    }

    pub fn is_need_split(&self) -> bool {
        return self.is_need_split;
    }

    fn get_write(export_path : &str, file_num : i32, is_gzip : bool) -> Result<Box<dyn Write>, Error> {
        let path = Path::new(&export_path);
        let mut new_path = PathBuf::new();
        let mut new_file_name;

        if let Some(parent) = path.parent() {
            new_path.push(parent);
        }

        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            new_file_name = stem.to_owned();
            new_file_name.push_str(&format!(".{:09}", file_num));

            if let Some(extension) = path.extension().and_then(|s| s.to_str()) {
                new_file_name.push('.');
                new_file_name.push_str(extension);
            }

            if is_gzip {
                let mut new_file_name_with_gz = new_file_name.clone();
                new_file_name_with_gz.push_str(".gz");
                new_path.push(new_file_name_with_gz);
            } else {
                new_path.push(new_file_name.clone());
            }
        } else {
            return Err(Error::Other("invalid file path.".to_string()));
        }
    
        let file = match std::fs::File::create(new_path) {
            Ok(file) => file,
            Err(e)  => {
                return Err(Error::Other(e.to_string()));
            }
        };
        
        if is_gzip {
            let gz_encoder = flate2::GzBuilder::new()
                                                .filename(new_file_name)
                                                .comment("tidb table dumped data")
                                                .mtime(chrono::Utc::now().timestamp() as u32)
                                                .write(file, flate2::Compression::default());
            return Ok(Box::new(gz_encoder));
        }
        else {
           return Ok(Box::new(file));
        }
    }
}

impl Write for WriteWrap {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {

        let res: Result<usize, std::io::Error> = self.cur_write.write(buf);
        if res.is_ok() {
            self.cur_file_writed_size += res.as_ref().unwrap();
        }

        if self.file_size > 0 && self.cur_file_writed_size > self.file_size {
            self.is_need_split = true;
        }

        return res;
    }

    fn flush(&mut self) -> std::io::Result<()> {
        return self.cur_write.flush();
    }
}