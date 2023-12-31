use rocksdb::DBIterator;
use tidb_query_datatype::codec::table::decode_int_handle;

use crate::{tidbtypes::TableInfo, errors::Error};
use crate::datum::RowData;
use txn_types::{WriteRef, WriteType, TimeStamp};

pub struct TableDataIterator<'a, 'b> {
    table_info : &'a TableInfo,

    next_readed_row_data_buf : Option<Box<RowData>>,
    next_data_cf_default_buf : Option<Box<RowData>>,
    next_data_cf_write_buf : Option<(Box<[u8]>, Box<[u8]>)>,

    table_data_cf_default_iter : DBIterator<'b>,
    table_data_cf_default_returned_eof : bool,

    table_data_cf_write_iter : DBIterator<'b>,
    table_data_cf_write_returned_eof : bool,
}

impl <'a, 'b> TableDataIterator<'a, 'b> {
    pub fn new(table_info : &'a TableInfo, table_data_cf_default_iter : DBIterator<'b>, table_data_cf_write_iter : DBIterator<'b>) -> TableDataIterator<'a, 'b> {
        return TableDataIterator {
            table_info,
            next_readed_row_data_buf : None,
            next_data_cf_default_buf : None,
            next_data_cf_write_buf : None,

            table_data_cf_default_iter,
            table_data_cf_default_returned_eof : false,

            table_data_cf_write_iter,
            table_data_cf_write_returned_eof : false,
        };
    }

    fn get_inner_row_data_from_default(&mut self) -> Option<Result<Box<RowData>, Error>> {
        if let Some(row_data) = self.next_data_cf_default_buf.take() {
            return Some(Result::Ok(row_data));
        }

        if self.table_data_cf_default_returned_eof {
            return None;
        } else {
            return match self.table_data_cf_default_iter.next() {
                None => {
                    self.table_data_cf_default_returned_eof = true;
                    None
                },
                Some(res) => Some({
                    match res {
                        Err(e) => Result::Err(Error::CorruptedData(e.to_string())),
                        Ok((key_data, val_data)) => {
                            let mut key_data_ref = &key_data[1..];
                            match tikv_util::codec::bytes::decode_bytes(&mut key_data_ref, false) {
                                Ok(key_decoded_data) => {
                                    match RowData::new(key_decoded_data.into_boxed_slice(), val_data, self.table_info) {
                                        Ok(row_data) => Ok(Box::new(row_data)),
                                        Err(e) => Err(e),
                                    }
                                },
                                Err(_) => {
                                    Err(Error::CorruptedDataBytes("key data decode error.".to_string(), key_data))
                                },
                            }
                        }
                    }//end of match res
                })
            };//return match self.table_data_cf_default_iter.next()
        }
    }

    fn get_inner_write_data_from_write(&mut self) -> Option<Result<(Box<[u8]>,Box<[u8]>), Error>> {
        if let Some(data_pair) = self.next_data_cf_write_buf.take() {
            return Some(Ok(data_pair));
        }

        if self.table_data_cf_write_returned_eof {
            return None;
        }

        //get data from iter
        match self.table_data_cf_write_iter.next() {
            None => {
                self.table_data_cf_write_returned_eof = true;
                return None;
            },
            Some(res) => match res {
                Ok((raw_key_data, val_data)) => {
                    let mut key_data_ref = &raw_key_data[1..];
                    if let Ok(key_decoded_data) = tikv_util::codec::bytes::decode_bytes(&mut key_data_ref, false) {
                        return Some(Ok((key_decoded_data.into_boxed_slice(), val_data)));
                    } else {
                        return Some(Err(Error::CorruptedDataBytes("key data decode error.".to_string(), raw_key_data)));
                    }
                },
                Err(e) => return Some(Err(Error::CorruptedData(e.into_string()))),
            }
        }
    }


    fn get_inner_row_data(&mut self) -> Option<Result<Box<RowData>, Error>> {
        let mut cur_handle_id : i64 = 0;
        let mut cur_row_data : Option<Box<RowData>> = None;
        let mut max_delete_ts : TimeStamp = TimeStamp::zero();

        loop {
            if self.next_readed_row_data_buf.is_some() {
                cur_row_data = self.next_readed_row_data_buf.take();
                cur_handle_id = cur_row_data.as_ref().unwrap().handle_int;
            }

            //if cf default had read all out.
            if !self.table_data_cf_default_returned_eof {
                loop {
                    let row_data_op = self.get_inner_row_data_from_default();
                    match row_data_op {
                        None => break,
                        Some(res) => {
                            if res.is_err() {
                                return Some(res);
                            }
                            let row_data = res.unwrap();
                            if cur_handle_id == 0 {
                                cur_handle_id = row_data.handle_int;
                                cur_row_data = Some(row_data);
                            } else if cur_handle_id == row_data.handle_int {
                                //skip old version
                                //print!("skip id: {}\n", cur_handle_id);
                                continue;
                            } else {
                                //cache next row_data
                                self.next_data_cf_default_buf = Some(row_data);
                                break;
                            }
                        }
                    }
                }//end of row_data loop
            }

            //get data from write family
            //it will be ended by none or older handle id
            loop {
                let write_data_opt = self.get_inner_write_data_from_write();
                if write_data_opt.is_none() {
                    break;
                }

                let write_data_res = write_data_opt.unwrap();
                if write_data_res.is_err() {
                    return Some(Err(write_data_res.err().unwrap()));
                }

                let (key_data, val_data) = write_data_res.ok().unwrap();
                
                let handle_int = match decode_int_handle(key_data.as_ref()) {
                    Ok(handle_int) => handle_int,
                    Err(_) => return Some(Err(Error::CorruptedDataBytes("decode handle int from key data error.".to_string(), key_data))),
                };

                if handle_int < cur_handle_id {
                    //move newer data to next buffer, to operate later.
                    //the new id would be currect.
                    if !cur_row_data.is_none() {
                        self.next_readed_row_data_buf = cur_row_data.take();
                        cur_handle_id = handle_int;
                    } else {
                        // error
                        continue;
                    }
                } else if handle_int > cur_handle_id && cur_handle_id != 0 {
                    //we will operate next time
                    self.next_data_cf_write_buf = Some((key_data, val_data));
                    break;
                }

                //do something
                let wref = match WriteRef::parse(val_data.as_ref()) {
                    Ok(wref) => wref,
                    Err(_) => return Some(Err(Error::CorruptedDataBytes("parse WriteRef error.".to_string(), val_data))),
                };
                if wref.write_type == WriteType::Delete {
                    let append_ts = wref.start_ts;
                    if append_ts > max_delete_ts {
                        max_delete_ts = append_ts;
                    }
                } else if wref.write_type == WriteType::Put {
                    if wref.short_value.is_none() {
                        continue;
                    }
                    
                    let row_data = Box::new(
                        match RowData::new(key_data, Box::<[u8]>::from(wref.short_value.unwrap()), self.table_info){
                            Ok(row_data) => row_data,
                            Err(e) => return Some(Err(e)),
                        });
                    if cur_row_data.is_none() || row_data.append_ts > cur_row_data.as_ref().unwrap().append_ts {
                        cur_handle_id = row_data.handle_int;
                        cur_row_data = Some(row_data);
                    }
                }
            }

            if cur_row_data.is_some() {
                if max_delete_ts >= cur_row_data.as_ref().unwrap().append_ts {
                    //data is deleted
                    //print!("{} deleted\n", cur_handle_id);
                    cur_row_data = None;
                } else {
                    return Some(Ok(cur_row_data.unwrap()));
                }
            }

            //ready to read next id
            cur_handle_id = 0;
            max_delete_ts = TimeStamp::zero();

            //read over all
            if self.table_data_cf_default_returned_eof && self.table_data_cf_write_returned_eof {
                break;
            }
        }

        return None;
    }
}

impl <'a, 'b> Iterator for TableDataIterator<'a,'b> {
    type Item = Result<Box<RowData>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.get_inner_row_data() {
            None => None,
            Some(res) => Some(res),
        }
    }
}