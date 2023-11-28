use std::{sync::{Mutex, Arc}, cell::RefCell, rc::Rc, io::Write, thread::{JoinHandle, self}};

use crossbeam_channel::Receiver;
use csv::Writer;
use tidb_query_datatype::FieldTypeTp;

use crate::{errors::{Error, self}, datum::{DatumRef, RowData}, tidbtypes::TableInfo};

use super::{FileWriteWrap, buf::LinkedBuffer, LinkedBufferWrapper, exporter::{TiDBFileExporter, TiDBExporter}};


pub struct CsvExporter {
    fw : Arc<Mutex<FileWriteWrap>>,
    table_info : TableInfo,
    thread_num : usize,
    is_debug_mode : bool,
}

impl CsvExporter {
    pub fn new(table_info : TableInfo, write_path : &str, maximum_file_size_mb : usize, is_gzip : bool) -> CsvExporter {
        match Self::create_file_write_wrap(write_path, maximum_file_size_mb, is_gzip) {
            Ok(fw) => {
                return CsvExporter {
                    table_info,
                    fw : Arc::new(Mutex::new(fw)),
                    thread_num : 3,
                    is_debug_mode : false,
                }
            },
            Err(e) => panic!("{}", e.to_string()),
        }
    }
}

impl TiDBFileExporter for CsvExporter {}
impl TiDBExporter for CsvExporter {
    fn start_export(&mut self, rx : Receiver<Vec<Box<RowData>>>) -> Vec<JoinHandle<()>> {
        let mut handlers = Vec::with_capacity(self.thread_num);
        for _ in 0..self.thread_num {
            let fw_arc = self.fw.clone();
            let rx_thread = rx.clone();
            let table_info = self.table_info.clone();
            let is_debug_mode = self.is_debug_mode;
            let handle = thread::spawn(move || {
                let mut export_writer = Box::new(CsvWriter::new(&fw_arc));
                for blocks in rx_thread {
                    for row_data in blocks {
                        if let Err(e) = export_writer.write_row_data(row_data, &table_info) {
                            print!("{}", e.to_string());
                            if is_debug_mode {
                                errors::display_corrupted_err_data(&e);
                            }
                            panic!("{:?}", e);
                        }
                    }
                }
                _ = export_writer.flush();
            });
            handlers.push(handle);
        }

        return handlers;
    }

    fn set_thread_num(&mut self, num : usize) {
        if num > 0 {
            self.thread_num = num;
        }
    }

    fn set_debug_mode(&mut self, is_debug : bool) {
        self.is_debug_mode = is_debug
    }
}


pub struct CsvWriter<'b> {
    csv_writer : Writer<LinkedBufferWrapper>,
    writed_row_num : usize,
    buffer : Rc<RefCell<LinkedBuffer>>,
    fw : &'b Mutex<FileWriteWrap>,
}

impl CsvWriter<'_> {
    pub fn new<'b>(fw : &'b Mutex<FileWriteWrap>) -> CsvWriter<'b> {
        let buf = Rc::new(RefCell::new(LinkedBuffer::new(1024 * 1024 * 10, 5, false)));//100MB
        
        let csv_writer = match Self::get_inner_csv_writer(buf.clone()) {
            Ok(w) => w,
            Err(e) => panic!("{:?}", e),
        };
        return CsvWriter {
            csv_writer,
            writed_row_num : 0,
            buffer : buf.clone(),
            fw
        };
    }

    fn is_not_need_quote(&self, d : &DatumRef) -> bool {
        match d.get_field_tp() {
            FieldTypeTp::Null
            | FieldTypeTp::Float
            | FieldTypeTp::NewDecimal
            | FieldTypeTp::Double
            | FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong => true,
            _ => false
        }
    }

    fn get_inner_csv_writer(buf : Rc<RefCell<LinkedBuffer>>) -> Result<csv::Writer<LinkedBufferWrapper>, Error> {

        let csv_writer = csv::WriterBuilder::new()
            .double_quote(false)
            .quote_style(csv::QuoteStyle::Never)
            .from_writer(LinkedBufferWrapper::new(buf));

        return Ok(csv_writer);
    }

    fn write_row_data(&mut self, row_data : Box<RowData>, table_info : &TableInfo) -> Result<(), Error> {
        let datum_refs = row_data.get_datum_refs(table_info)?;

        let mut data_record = csv::StringRecord::with_capacity(1024, datum_refs.len());

        for d in datum_refs {
            let mut field_str = &d.try_to_string()?;
            if d.is_null() {
                data_record.push_field("\\N");
                continue;
            }

            if self.is_not_need_quote(&d) {
                data_record.push_field(field_str);
                continue;
            }
            
            let mut escaped_field_str;
            if field_str.contains("\\") {
                escaped_field_str = field_str.replace("\\", "\\\\");
                field_str = &escaped_field_str;
            }
            if field_str.contains("\n") {
                escaped_field_str = field_str.replace("\n", "\\n");
                field_str = &escaped_field_str;
            } 
            
            if field_str.contains("\r") {
                escaped_field_str = field_str.replace("\r", "\\r");
                field_str = &escaped_field_str;
            }

            if field_str.contains("\"") {
                escaped_field_str = field_str.replace("\"", "\\\"");
                field_str = &escaped_field_str;
            }
            
            let mut quoted_str = String::with_capacity(2 + field_str.len());
            quoted_str.push_str("\"");
            quoted_str.push_str(field_str);
            quoted_str.push_str("\"");
            data_record.push_field(&quoted_str);
            
            //print!("***\nfield:{}\ndata:{}\n", d.get_column().name.L, d.to_string());
        }

        let res = self.csv_writer.write_record(&data_record);
        if res.is_err() {
            return Err(Error::Other(res.err().unwrap().to_string()));
        }
        self.writed_row_num += 1;

        if self.writed_row_num % 100 == 0 {
            let buffer_ref = self.buffer.borrow();
            if buffer_ref.is_full() {
                drop(buffer_ref);
                _ = self.csv_writer.flush();
                if let Ok(mut fw) = self.fw.lock() {
                    
                    if fw.is_exceed_file_size() {
                        _ = fw.generate_next_file();
                    }

                    if let Err(e) = self.buffer.borrow().write_to(fw.by_ref()) {
                        return Err(Error::IO(e.to_string()));
                    }
                    (*(self.buffer)).borrow_mut().reset();
                } else {
                    return Err(Error::Other("lock for writing failed.".to_string()));
                }
            }
        }

        return Ok(());
    }

    fn flush(&mut self) -> Result<(), Error> {
        _ = self.csv_writer.flush();
        
        if let Ok(mut fw) = self.fw.lock() {
            if self.buffer.borrow().len() > 0 {

                if fw.is_exceed_file_size() {
                    _ = fw.generate_next_file();
                }

                if let Err(e) = self.buffer.borrow().write_to(fw.by_ref()) {
                    return Err(Error::IO(e.to_string()));
                }
                (*(self.buffer)).borrow_mut().reset();
            }

            if let Err(e) = fw.flush() {
                return Err(Error::IO(e.to_string()));
            }

            return Ok(());
        } else {
            return Err(Error::Other("lock for writing failed.".to_string()));
        }
    }
}