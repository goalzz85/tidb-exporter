use csv::Writer;
use tidb_query_datatype::FieldTypeTp;

use crate::{writer::TiDBExportWriter, datum::{RowData, DatumRef}, errors::Error, tidbtypes::TableInfo};

use super::WriteWrap;

#[allow(unused_variables, dead_code)]
pub struct CsvWriter<'a> {
    table_info : &'a TableInfo,
    export_path : String,
    file_size : usize,
    is_gzip : bool,
    csv_writer : Writer<WriteWrap>,
    cur_file_num : i32,
}

impl CsvWriter<'_> {
    pub fn new<'a>(table_info : &'a TableInfo, export_path : &str, file_size : usize, is_gzip : bool) -> CsvWriter<'a> {
        let mut cur_file_num = 0;
        if file_size > 0 {
            cur_file_num = 1;
        }
        let csv_writer = match Self::get_inner_csv_writer(table_info, export_path, file_size, cur_file_num, is_gzip) {
            Ok(w) => w,
            Err(e) => panic!("{:?}", e),
        };
        return CsvWriter {
            table_info : table_info,
            export_path : export_path.to_owned(),
            file_size : file_size,
            is_gzip : is_gzip,
            csv_writer : csv_writer,
            cur_file_num : cur_file_num,
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

    fn get_inner_csv_writer(table_info : &TableInfo, export_path : &str, file_size : usize, file_num : i32, is_gzip : bool) -> Result<csv::Writer<WriteWrap>, Error> {
        let write_wrap = WriteWrap::new(export_path, file_size, file_num, is_gzip)?;

        let mut csv_writer = csv::WriterBuilder::new()
            .double_quote(false)
            .quote_style(csv::QuoteStyle::Never)
            .from_writer(write_wrap);
    
        //write title
        let mut title_record = csv::StringRecord::new();
        for col in &table_info.cols {
            title_record.push_field(&col.name.L);
        }
        
        csv_writer.write_record(&title_record).unwrap();

        return Ok(csv_writer);
    }
}

impl TiDBExportWriter for CsvWriter<'_> {
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

        return Ok(());
    }

    fn check_split_file(&mut self) -> Result<(), Error> {
        if !self.csv_writer.get_ref().is_need_split() {
            return Ok(());
        }
        self.cur_file_num += 1;
        self.csv_writer = Self::get_inner_csv_writer(self.table_info, &self.export_path, self.file_size, self.cur_file_num, self.is_gzip)?;
        return Ok(());
    }

    fn flush(&mut self) {
        _ = self.csv_writer.flush();
    }
}