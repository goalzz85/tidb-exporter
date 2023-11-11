use std::{cell::RefCell, rc::Rc, io::Write};

use csv::Writer;
use tidb_query_datatype::FieldTypeTp;

use crate::{writer::TiDBExportWriter, datum::{RowData, DatumRef}, errors::Error, tidbtypes::TableInfo};

use super::{FileWriteWrap, buf::LinkedBuffer};

struct LinkedBufferWrapper {
    buf : Rc<RefCell<LinkedBuffer>>
}

impl LinkedBufferWrapper {
    pub fn new(buf : Rc<RefCell<LinkedBuffer>>) -> LinkedBufferWrapper {
        return LinkedBufferWrapper { buf }
    }
}

impl Write for LinkedBufferWrapper {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buf.borrow_mut().flush()
    }
}


#[allow(unused_variables, dead_code)]
pub struct CsvWriter<'a> {
    table_info : &'a TableInfo,
    csv_writer : Writer<LinkedBufferWrapper>,
    writed_row_num : usize,
    buffer : Rc<RefCell<LinkedBuffer>>,
    fw : FileWriteWrap,
}

impl CsvWriter<'_> {
    pub fn new<'a>(table_info : &'a TableInfo,fw : FileWriteWrap, buffer_size_mb : usize) -> CsvWriter<'a> {
        let buffer_size_mb = if buffer_size_mb <= 0 {
            10
        } else {
            buffer_size_mb
        };

        let buf = Rc::new(RefCell::new(LinkedBuffer::new(1024 * 1024 * 10, 200)));
        
        let csv_writer = match Self::get_inner_csv_writer(buf.clone(), buffer_size_mb) {
            Ok(w) => w,
            Err(e) => panic!("{:?}", e),
        };
        return CsvWriter {
            table_info,
            csv_writer,
            writed_row_num : 0,
            buffer : buf.clone(), //500MB
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

    fn get_inner_csv_writer(buf : Rc<RefCell<LinkedBuffer>>, buffer_size : usize) -> Result<csv::Writer<LinkedBufferWrapper>, Error> {

        let csv_writer = csv::WriterBuilder::new()
            .double_quote(false)
            .quote_style(csv::QuoteStyle::Never)
            .buffer_capacity(buffer_size * 1024 * 1024)
            .from_writer(LinkedBufferWrapper::new(buf));

        return Ok(csv_writer);
    }
}

impl TiDBExportWriter for CsvWriter<'_> {
    fn writed_row_num(&self) -> usize {
        return self.writed_row_num;
    }

    fn write_row_data(&mut self, row_data : Box<RowData>) -> Result<(), Error> {
        let datums_res = row_data.get_datum_refs();
        if datums_res.is_err() {
            //print!("{}", datums_res.err().unwrap());
            return Err(Error::CorruptedData("row data to datum_refs error".to_owned()));
        }

        let datum_refs = datums_res.unwrap();

        let mut data_record = csv::StringRecord::with_capacity(1024, datum_refs.len());

        for d in datum_refs {
            let mut field_str = &d.to_string();
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
            //TODO
            
            if self.buffer.borrow().len() > self.fw.maximum_file_size() {
                _ = self.csv_writer.flush();
                _ = self.buffer.borrow().write_to(&mut self.fw);
                self.buffer.borrow_mut().reset();
                _ = self.fw.flush();
            }
        }

        return Ok(());
    }

    fn flush(&mut self) {
        _ = self.csv_writer.flush();
    }
}