
use std::{io::Write, path::Path};

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
    pub fn new(export_path : &str, file_size : usize, file_num : i32, is_gzip : bool) -> WriteWrap {
        let w = Self::get_write(export_path, file_num, is_gzip);
        return WriteWrap {
            file_size : file_size,
            cur_file_writed_size : 0,
            cur_write : w,
            is_need_split : false,
        };
    }

    pub fn is_need_split(&self) -> bool {
        return self.is_need_split;
    }

    fn get_write(export_path : &str, file_num : i32, is_gzip : bool) -> Box<dyn Write> {
        let mut export_file_path : String;
        let mut file_num_str = "".to_owned();
        if file_num > 0 {
            file_num_str = format!(".{:09}", file_num);
        }
        export_file_path = format!("{}{}", export_path, file_num_str);

        if is_gzip {
            export_file_path = export_file_path + ".gz";
        }

        let path = Path::new(&export_file_path);
        let export_file_name = path.file_name().unwrap().to_str().unwrap();
    
        let file = std::fs::File::create(export_file_path.as_str()).unwrap();
        
        if is_gzip {
            let gz_encoder = flate2::GzBuilder::new()
                                                .filename(&export_file_name[0..export_file_name.len()-3])
                                                .comment("tidb table dumped data")
                                                .mtime(chrono::Utc::now().timestamp() as u32)
                                                .write(file, flate2::Compression::default());
            return Box::new(gz_encoder);
        }
        else {
           return Box::new(file);
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