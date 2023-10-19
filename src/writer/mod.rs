
use std::io::Write;

use crate::datum::RowData;

use super::errors::Error;

pub mod csvwriter;

pub trait TiDBExportWriter {
    fn write_row_data(&mut self, row_data : Box<RowData>) -> Result<(), Error>;
    fn flush(&mut self);
}

pub struct WriteWrap {
    file_size : usize,
    is_gzip : bool,
    export_path : String,
    cur_file_num : i32,
    cur_file_writed_size : usize,
    cur_write : Box<dyn std::io::Write>,

    writed_size : usize,
}

impl WriteWrap {
    pub fn new(export_path : &str, file_size : usize, is_gzip : bool) -> WriteWrap{

        let w = Self::get_write(export_path, 1, is_gzip);
        return WriteWrap {
            file_size : file_size,
            is_gzip : is_gzip,
            export_path : export_path.to_owned(),
            cur_file_num : 1,
            cur_file_writed_size : 0,
            cur_write : w,
            writed_size : 0,
        };
    }


    fn get_write(export_path : &str, file_num : i32, is_gzip : bool) -> Box<dyn Write> {
        let mut export_filename : String;
        let mut file_num_str = "".to_owned();
        if file_num > 0 {
            file_num_str = format!(".{:09}", file_num);
        }
        export_filename = format!("{}{}", export_path, file_num_str);

        if is_gzip {
            export_filename = export_filename + ".gz";
        }
    
        let file = std::fs::File::create(export_filename.as_str()).unwrap();
        
        if is_gzip {
            let gz_encoder = flate2::GzBuilder::new()
                                                .filename(&export_filename[0..export_filename.len()-3])
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

        if self.file_size > 0 && self.cur_file_writed_size > self.file_size {
            //new file
            self.cur_write.flush()?;

            self.cur_file_writed_size = 0;
            self.cur_file_num += 1;

            self.cur_write = Self::get_write(&self.export_path, self.cur_file_num, self.is_gzip);
        }

        let res = self.cur_write.write(buf);
        if res.is_ok() {
            self.writed_size += res.as_ref().unwrap();
        }

        return res;
    }

    fn flush(&mut self) -> std::io::Result<()> {
        return self.cur_write.flush();
    }
}