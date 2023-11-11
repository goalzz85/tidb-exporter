
use std::{io::Write, path::{Path, PathBuf}, fs::File};

use flate2::write::GzEncoder;

use crate::datum::RowData;

use super::errors::Error;

mod buf;
pub mod csvwriter;

pub trait TiDBExportWriter {
    fn write_row_data(&mut self, row_data : Box<RowData>) -> Result<(), Error>;
    fn flush(&mut self);
    fn writed_row_num(&self) -> usize;
}

pub trait FileWrite : Write {
    fn writed_size(&self) -> usize;
}

struct RawFileWrap {
    writed_size : usize,
    fd : File,
}

impl RawFileWrap {
    pub fn new(fd : File) -> RawFileWrap {
        return RawFileWrap {
            writed_size : 0,
            fd : fd,
        };
    }
}

impl FileWrite for RawFileWrap {
    fn writed_size(&self) -> usize {
        return self.writed_size;
    }
}

impl Write for RawFileWrap {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.fd.write(buf){
            Ok(ws) => {
                self.writed_size += ws;
                return Ok(ws);
            },
            Err(e) => return Err(e),
        };
    }

    fn flush(&mut self) -> std::io::Result<()> {
        //ensure all data of in-memory have been writed into disk.
        return self.fd.sync_all()
    }
}

pub struct GzFileWrap {
    fw : GzEncoder<RawFileWrap>,
}

impl GzFileWrap {
    fn new(fw : RawFileWrap, file_name : &str) -> GzFileWrap {
        let gz_encoder = flate2::GzBuilder::new()
                                                .filename(file_name)
                                                .comment("tidb table dumped data")
                                                .mtime(chrono::Utc::now().timestamp() as u32)
                                                .write(fw, flate2::Compression::default());
        return GzFileWrap { fw:gz_encoder }
    }
}

impl FileWrite for GzFileWrap {
    fn writed_size(&self) -> usize {
        return self.fw.get_ref().writed_size;
    }
}

impl Write for GzFileWrap {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        return self.fw.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        //ensure all data of in-memory have been writed into disk.
        return self.fw.flush();
    }
}

pub struct FileWriteWrap {
    export_path : String,
    maximum_file_size : usize,
    cur_write : Box<dyn FileWrite>,
    cur_file_num : i32,
    is_gzip : bool,
    is_need_flush : bool,
}

impl FileWriteWrap {
    pub fn new(export_path : &str, maximum_file_size : usize, is_gzip : bool) -> Result<FileWriteWrap, Error> {
        let mut file_num = 0;
        if maximum_file_size > 0 {
            file_num = 1;
        }

        let w = Self::get_write(export_path, file_num, is_gzip)?;

        return Ok(FileWriteWrap {
            export_path : export_path.to_string(),
            maximum_file_size : maximum_file_size,
            cur_file_num : file_num,
            cur_write : w,
            is_gzip : is_gzip,
            is_need_flush : false,
        });
    }

    fn get_write(export_path : &str, file_num : i32, is_gzip : bool) -> Result<Box<dyn FileWrite>, Error> {
        let path = Path::new(&export_path);
        let mut new_path = PathBuf::new();
        let mut new_file_name;

        if let Some(parent) = path.parent() {
            new_path.push(parent);
        }

        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            new_file_name = stem.to_owned();
            if file_num > 0 {
                new_file_name.push_str(&format!(".{:09}", file_num));
            }

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
    
        let fw = match std::fs::File::create(new_path) {
            Ok(file) => RawFileWrap::new(file),
            Err(e)  => {
                return Err(Error::Other(e.to_string()));
            }
        };
        
        if is_gzip {
            return Ok(Box::new(GzFileWrap::new(fw, &new_file_name)));
        }
        else {
           return Ok(Box::new(fw));
        }
    }

    #[allow(dead_code)]
    pub fn is_need_flush(&self) -> bool {
        return self.is_need_flush;
    }

    pub fn generate_next_file(&mut self) {
        let file_num = self.cur_file_num + 1;
        if let Ok(fw) = Self::get_write(&self.export_path, file_num, self.is_gzip) {
            self.cur_write = fw;
            self.cur_file_num = file_num;
            self.is_need_flush = false;
            return;
        }

        panic!("create new file to export failed");
    }

    pub fn maximum_file_size(&self) -> usize {
        self.maximum_file_size
    }
}

impl Write for FileWriteWrap {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let res: Result<usize, std::io::Error> = self.cur_write.write(buf);
        if self.maximum_file_size > 0 && self.cur_write.writed_size() > self.maximum_file_size {
            self.is_need_flush = true;
        }
        return res;
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.maximum_file_size > 0 && self.cur_write.writed_size() > self.maximum_file_size {
            self.generate_next_file();
        }
        return self.cur_write.flush();
    }
}