#![allow(unused_variables, dead_code)]
use std::marker::PhantomData;
use std::cmp::Ordering::{Equal, Greater, Less};
use num_traits::PrimInt;
use tidb_query_datatype::codec::table::decode_int_handle;
use tidb_query_datatype::expr::EvalContext;

use codec::prelude::{NumberDecoder, BufferReader};
use tidb_query_datatype::codec::data_type::JsonRef;
use tidb_query_datatype::codec::datum_codec::DatumPayloadDecoder;
use tidb_query_datatype::codec::mysql::{JsonType, Decimal, Duration, Time, TimeType};
use tidb_query_datatype::{codec::row::v2::*, FieldTypeTp};
use tidb_query_datatype::codec::{Result, Error};
use codec::number::NumberCodec;
use crate::tidbtypes::{TableInfo, ColumnInfo};
use txn_types::{Key, TimeStamp};


//some code are copyed from tikv RowSlice
pub struct RowData {
    pub handle_int : i64,
    pub append_ts : TimeStamp,
    key_data : Box<[u8]>,
    val_data : Box<[u8]>,
    pri_data : Box<[u8]>,
}

impl RowData {
    pub fn new(key_data : Box<[u8]>, val_data : Box<[u8]>, table_info : &TableInfo) -> RowData {
        
        let key = Key::from_raw(key_data.as_ref());
        let handle_int = decode_int_handle(key_data.as_ref()).unwrap();
        let mut row_data = RowData {
            handle_int : handle_int,
            append_ts : key.decode_ts().unwrap(),
            key_data: key_data,
            val_data: val_data,
            pri_data: Box::new([0; 8]),
        };

        if table_info.pk_is_handle {
            for col in &table_info.cols {
                if col.field_type.has_prikey_flag() {
                    row_data.write_pri_data(col);
                }
            }
        }

        return row_data;
    }

    pub fn get_datum_refs<'a, 'b> (&'b self, table_info : &'a TableInfo) -> Result<Vec<DatumRef<'b, 'a>>> {
        let mut data = self.val_data.as_ref();
        assert_eq!(data.read_u8()?, CODEC_VERSION);
        let is_big = data.read_u8()? & 1 == 1;
        if is_big {
            return self.get_datum_refs_as_big(data, table_info);
        } else {
            return self.get_datum_refs_as_small(data, table_info);
        }
    }

    fn write_pri_data(& mut self, col : &ColumnInfo) {
        if col.field_type.is_unsigned() {
            self.pri_data.copy_from_slice(&(self.handle_int as u64).to_le_bytes());
        } else {
            self.pri_data.copy_from_slice(&(self.handle_int as i64).to_le_bytes());
        }
    }

    fn get_datum_refs_as_small<'a, 'b> (&'b self, mut data : &'b [u8], table_info : &'a TableInfo) -> Result<Vec<DatumRef<'b, 'a>>> {
        let non_null_cnt = data.read_u16_le()? as usize;
        let null_cnt = data.read_u16_le()? as usize;
        let non_null_ids : LeBytes<'b, u8> = read_le_bytes(&mut data, non_null_cnt)?;
        let null_ids : LeBytes<'b, u8> = read_le_bytes(&mut data, null_cnt)?;
        let offsets : LeBytes<'b, u16> = read_le_bytes(&mut data, non_null_cnt)?;
        let values : LeBytes<'b, u8> = LeBytes::new(data);

        let cols = &table_info.cols;
        let mut datum_list = Vec::with_capacity(cols.len());

        for col in cols {
            if let Ok(idx) = non_null_ids.binary_search(&(col.id as u8)) {
                let offset = offsets.get(idx).ok_or(Error::ColumnOffset(idx))?;
                let start = if idx > 0 {
                    // Previous `offsets.get(idx)` indicates it's ok to index `idx - 1`
                    unsafe { offsets.get_unchecked(idx - 1) as usize }
                } else {
                    0usize
                };
                let col_val = &values.slice[start..offset as usize];
                let datum_ref = DatumRef::parse_from(col_val, col);
                datum_list.push(datum_ref);
            } else if null_ids.binary_search(&(col.id as u8)).is_ok() {
                datum_list.push(DatumRef::get_null(col));
            } else {
                // This column is missing. It will be filled with default values
                // later.
                if table_info.pk_is_handle && col.field_type.has_prikey_flag() {
                    let datum_ref = DatumRef::parse_from(self.pri_data.as_ref(), col);
                    datum_list.push(datum_ref);
                } else {
                    datum_list.push(DatumRef::get_null(col));
                }
            }
        }
        return Ok(datum_list);
    }

    fn get_datum_refs_as_big <'a, 'b> (&'b self, mut data : &'b [u8], table_info : &'a TableInfo) -> Result<Vec<DatumRef<'b, 'a>>> {
        let non_null_cnt = data.read_u16_le()? as usize;
        let null_cnt = data.read_u16_le()? as usize;
        let non_null_ids : LeBytes<'b, u32> = read_le_bytes(&mut data, non_null_cnt)?;
        let null_ids : LeBytes<'b, u32> = read_le_bytes(&mut data, null_cnt)?;
        let offsets : LeBytes<'b, u32> = read_le_bytes(&mut data, non_null_cnt)?;
        let values : LeBytes<'b, u8> = LeBytes::new(data);

        let cols = &table_info.cols;
        let mut datum_list = Vec::with_capacity(cols.len());

        for col in cols {
            if let Ok(idx) = non_null_ids.binary_search(&(col.id as u32)) {
                let offset = offsets.get(idx).ok_or(Error::ColumnOffset(idx))?;
                let start = if idx > 0 {
                    // Previous `offsets.get(idx)` indicates it's ok to index `idx - 1`
                    unsafe { offsets.get_unchecked(idx - 1) as usize }
                } else {
                    0usize
                };
                let col_val = &values.slice[start..offset as usize];
                let datum_ref = DatumRef::parse_from(col_val, col);
                datum_list.push(datum_ref);
            } else if null_ids.binary_search(&(col.id as u32)).is_ok() {
                datum_list.push(DatumRef::get_null(col));
            } else {
                // This column is missing. It will be filled with default values
                // later.
                if table_info.pk_is_handle && col.field_type.has_prikey_flag() {
                    let datum_ref = DatumRef::parse_from(self.pri_data.as_ref(), col);
                    datum_list.push(datum_ref);
                } else {
                    datum_list.push(DatumRef::get_null(col));
                }
            }
        }
        return Ok(datum_list);
    }
}

pub fn parse_datum_refs<'a, 'b>(row_slice : &'a RowSlice, table_info : &'b TableInfo) -> Result<Vec<DatumRef<'a, 'b>>> {
    let cols = &table_info.cols;
    let mut datum_list = Vec::with_capacity(cols.len());
    for col in cols {
        if let Some((start, offset)) = row_slice.search_in_non_null_ids(col.id)? {
            let col_val = &row_slice.values()[start..offset];
            let datum_ref = DatumRef::parse_from(col_val, col);
            datum_list.push(datum_ref);
        } else if row_slice.search_in_null_ids(col.id) {
            datum_list.push(DatumRef::get_null(col));
        } else {
            datum_list.push(DatumRef::get_null(col));
        }
    }

    return Result::Ok(datum_list);
}

pub struct DatumRef<'a, 'b> {
    tp : FieldTypeTp,
    col : &'b ColumnInfo,
    data : &'a [u8],
}

impl <'a, 'b> DatumRef<'a, 'b> {

    pub fn parse_from(orgin_val : &'a [u8], column_info : &'b ColumnInfo) -> DatumRef<'a, 'b> {
        return DatumRef {
            tp: FieldTypeTp::from_u8(column_info.field_type.Tp).unwrap_or(FieldTypeTp::Unspecified),
            col: column_info,
            data: orgin_val
        };
    }

    pub fn get_null(column_info : &'b ColumnInfo) -> DatumRef<'static, 'b> {
        return DatumRef { tp: FieldTypeTp::Null, col: column_info, data: &[] }
    }

    pub fn get_column(&self) -> &ColumnInfo {
        return self.col;
    }

    pub fn as_u64(&self) -> Result<u64> {
        if !self.is_integer() || !self.col.field_type.is_unsigned() {
            return Result::Err(Error::CorruptedData("invalid u64 data".to_string()));
        }
        return decode_v2_u64(self.data);
    }

    pub fn as_i64(&self) -> Result<i64> {
        if !self.is_integer() || self.col.field_type.is_unsigned() {
            return Result::Err(Error::CorruptedData("invalid i64 data".to_string()));
        }

        return match self.data.len() {
            1 => Ok(i64::from(self.data[0] as i8)),
            2 => Ok(i64::from(NumberCodec::decode_u16_le(self.data) as i16)),
            4 => Ok(i64::from(NumberCodec::decode_u32_le(self.data) as i32)),
            8 => Ok(NumberCodec::decode_u64_le(self.data) as i64),
            _ => Err(Error::InvalidDataType(
                "Failed to decode row v2 data as i64".to_owned(),
            )),
        }
    }

    pub fn as_f32(&self) -> Result<f32> {
        if !self.is_float() {
            return Result::Err(Error::CorruptedData("invalid float data".to_string()));
        }

        let f_d = NumberCodec::decode_f64(self.data);

        return Result::Ok(f_d as f32);
    }

    pub fn as_double(&self) -> Result<f64> {
        if !self.is_float() {
            return Result::Err(Error::CorruptedData("invalid double data".to_string()));
        }
        let mut data = self.data;
        return data.read_datum_payload_f64();
    }

    pub fn as_decimal(&self) -> Result<Decimal> {
        if !self.is_decimal() {
            return Result::Err(Error::CorruptedData("invalid decimal data".to_string()));
        }

        let mut data = self.data;
        let res = data.read_datum_payload_decimal();
        if res.is_err() {
            return Result::Err(Error::CorruptedData("decode decimal data error".to_string()));
        }

        return Result::Ok(res.unwrap());
    }

    //will copy the data to heap.
    pub fn as_bytes(&self) -> Result<Box<[u8]>> {
        if !self.is_string() {
            return Result::Err(Error::CorruptedData("invalid bytes data".to_string()));
        }

        return Result::Ok(Box::from(self.data));
    }

    pub fn as_datetime(&self) -> Result<Time> {
        if !self.is_datatime() {
            return Result::Err(Error::CorruptedData("invalid datetime data".to_string()));
        }
        
        let time_type = if self.tp == FieldTypeTp::DateTime {
            TimeType::DateTime
        } else {
            TimeType::Date
        };
        
        let datetime_u64 = decode_v2_u64(self.data)?;
        let fsp = self.col.field_type.Decimal as i8;

        return Time::from_packed_u64(& mut EvalContext::default(), datetime_u64, time_type, fsp);
    }

    //UTC
    pub fn as_timestamp(&self) -> Result<Time> {
        if !self.is_timestamp() {
            return Result::Err(Error::CorruptedData("invalid timestamp data".to_string()));
        }
     
        let datetime_u64 = decode_v2_u64(self.data)?;
        return Time::from_packed_u64(
            & mut EvalContext::default(),
            datetime_u64,
            TimeType::Timestamp,
            self.col.field_type.Decimal as i8);
    }

    pub fn as_json_ref(&self) -> Result<JsonRef> {
        if !self.is_json() {
            return Result::Err(Error::CorruptedData("invalid json data".to_string()));
        }

        let json_type_res = JsonType::try_from(self.data[0]);
        if json_type_res.is_err() {
            return Result::Err(json_type_res.err().unwrap());
        }
        let j_ref = JsonRef::new(json_type_res.unwrap(), &self.data[1..]);
        return Result::Ok(j_ref);
    }

    fn as_enum_val(&self) ->Result<String> {
        if self.is_enum() {
            return Result::Err(Error::CorruptedData("invalid enum data".to_string()));
        }

        let num = decode_v2_u64(self.data)?;
        let idx = num as usize;
        if idx == 0 || idx > self.col.field_type.Elems.len().try_into().unwrap() {
            return Result::Err(Error::CorruptedData("enum data number overflow enum boundary".to_string()));
        }
        return Result::Ok(self.col.field_type.Elems[idx - 1].clone());
    }

    fn as_set_vals(&self) -> Result<Vec<String>> {
        if self.is_enum() {
            return Result::Err(Error::CorruptedData("invalid set data".to_string()));
        }
        let mut res = Vec::<String>::new();
        let num_res = decode_v2_u64(self.data);
        if num_res.is_err() {
            return Result::Err(Error::CorruptedData("invalid set data, decode to number error".to_string()));
        }
        let num = num_res.unwrap();

        if self.col.field_type.Elems.is_empty() {
            return Result::Ok(res);
        }

        for i in 0..self.col.field_type.Elems.len() {
            if (num & (1 << i)) > 0 {
                res.push(self.col.field_type.Elems[i].clone());
            }
        }

        return Result::Ok(res);
    }

    pub fn as_duration(&self) -> Result<Duration> {
        if !self.is_duration() {
            return Result::Err(Error::CorruptedData("invalid duration data".to_string()));
        }

        let nanos_res = self.as_i64();
        if nanos_res.is_err() {
            return Result::Err(Error::CorruptedData("invalid duration data".to_string()));
        }

        let nanos = nanos_res.unwrap();
        let fsp = self.col.field_type.Decimal as i8;
        let res = Duration::from_nanos(nanos, fsp);
        if res.is_err() {
            return Result::Err(Error::CorruptedData("invalid duration data".to_string()));
        }

        return Result::Ok(res.unwrap());
    }



    pub fn get_field_tp(&self) -> FieldTypeTp {
        return self.tp;
    }


    pub fn is_integer(&self) -> bool {
        match self.get_field_tp() {
            FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong => true,
            _ => false
        }
    }

    pub fn is_decimal(&self) -> bool {
        match self.get_field_tp() {
            FieldTypeTp::NewDecimal => true,
            _ => false
        }
    }

    fn is_float(&self) -> bool {
        match self.get_field_tp() {
            FieldTypeTp::Float => true,
            _ => false
        }
    }

    pub fn is_double(&self) -> bool {
        match self.get_field_tp() {
            FieldTypeTp::Double => true,
            _ => false
        }
    }

    pub fn is_year(&self) -> bool {
        match self.get_field_tp() {
            FieldTypeTp::Year => true,
            _ => false
        }
    }

    pub fn is_string(&self) -> bool {
        match self.get_field_tp() {
            FieldTypeTp::VarChar
            | FieldTypeTp::VarString
            | FieldTypeTp::String
            | FieldTypeTp::Geometry
            | FieldTypeTp::TinyBlob
            | FieldTypeTp::MediumBlob
            | FieldTypeTp::LongBlob
            | FieldTypeTp::Blob => true,
            _ => false
        }
    }

    pub fn is_datatime(&self) -> bool {
        match self.get_field_tp() {
            FieldTypeTp::Date
            | FieldTypeTp::DateTime => true,
            _ => false
        }
    }

    pub fn is_timestamp(&self) -> bool {
        match self.get_field_tp() {
            FieldTypeTp::Timestamp => true,
            _ => false
        }
    }

    pub fn is_duration(&self) -> bool {
        match self.get_field_tp() {
            FieldTypeTp::Duration => true,
            _ => false
        }
    }

    pub fn is_json(&self) -> bool {
        match self.get_field_tp() {
            FieldTypeTp::Json => true,
            _ => false
        }
    }

    pub fn is_enum(&self) -> bool {
        match self.get_field_tp() {
            FieldTypeTp::Enum => true,
            _ => false
        }
    }

    pub fn is_set(&self) -> bool {
        match self.get_field_tp() {
            FieldTypeTp::Set => true,
            _ => false
        }
    }

    pub fn is_null(&self) -> bool {
        match self.get_field_tp() {
            FieldTypeTp::Null => true,
            _ => false
        }
    }

    pub fn is_bit(&self) -> bool {
        match self.get_field_tp() {
            FieldTypeTp::Bit => true,
            _ => false
        }
    }

    //print data for test
    pub fn print(&mut self) {
        if self.is_decimal() {
            print!("{}\n", self.as_decimal().unwrap().to_string());
        }

        if self.is_string() {
            let s = String::from_utf8(self.as_bytes().unwrap().to_vec());
            print!("{}\n", s.unwrap());
        }

        if self.is_duration() {
            print!("{}\n", self.as_duration().unwrap().to_string());
        }

        if self.is_integer() {
            if self.col.field_type.is_unsigned() {
                print!("{}\n", self.as_u64().unwrap());
            } else {
                print!("{}\n", self.as_i64().unwrap());
            }
        }

        if self.is_double() {
            print!("{}\n", self.as_double().unwrap());
        }

        if self.is_float() {
            print!("{}\n", self.as_f32().unwrap());
        }

        if self.is_json() {
            print!("{}", self.as_json_ref().unwrap().to_string());
        }

        if self.is_enum() {
            print!("{}\n", self.as_enum_val().unwrap());
        }

        if self.is_set() {
            print!("{:?}\n", self.as_set_vals().unwrap());
        }
    }
}

impl ToString for DatumRef<'_, '_> {
    fn to_string(&self) -> String {
        if self.is_integer() {
            if self.col.field_type.is_unsigned() {
                return self.as_u64().unwrap().to_string();
            } else {
                return self.as_i64().unwrap().to_string();
            }
        } else if self.is_float() {
            return self.as_f32().unwrap().to_string();
        } else if self.is_string() {
            //to_do text encoding?
            return String::from_utf8(self.as_bytes().unwrap().to_vec()).unwrap();
        } else if self.is_decimal() {
            return self.as_decimal().unwrap().to_string();
        } else if self.is_double() {
            return self.as_double().unwrap().to_string();
        } else if self.is_duration() {
            return self.as_duration().unwrap().to_string();
        } else if self.is_enum() {
            return self.as_enum_val().unwrap();
        } else if self.is_set() {
            return "[".to_owned() + &self.as_set_vals().unwrap().join(",") + "]";
        } else if self.is_json() {
            return self.as_json_ref().unwrap().to_string();
        } else if self.is_timestamp() {
            return self.as_timestamp().unwrap().to_string();
        } else if self.is_datatime() {
            return self.as_datetime().unwrap().to_string();
        } else if self.tp == FieldTypeTp::Null {
            return "NULL".to_string();
        }

        return "NULL".to_string();
    }
}

#[cfg(target_endian = "little")]
#[inline]
fn read_le_bytes<'a, T>(buf: &mut &'a [u8], len: usize) -> Result<LeBytes<'a, T>>
where
    T: PrimInt,
{
    let bytes_len = std::mem::size_of::<T>() * len;
    if buf.len() < bytes_len {
        return Err(Error::unexpected_eof());
    }
    let slice = &buf[..bytes_len];
    buf.advance(bytes_len);
    Ok(LeBytes::new(slice))
}

pub struct LeBytes<'a, T: PrimInt> {
    slice: &'a [u8],
    _marker: PhantomData<T>,
}

#[cfg(target_endian = "little")]
impl<'a, T: PrimInt> LeBytes<'a, T> {
    fn new(slice: &'a [u8]) -> Self {
        Self {
            slice,
            _marker: PhantomData::default(),
        }
    }

    #[inline]
    fn get(&self, index: usize) -> Option<T> {
        if std::mem::size_of::<T>() * index >= self.slice.len() {
            None
        } else {
            unsafe { Some(self.get_unchecked(index)) }
        }
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> T {
        let ptr = self.slice.as_ptr() as *const T;
        let ptr = ptr.add(index);
        std::ptr::read_unaligned(ptr)
    }

    #[inline]
    fn binary_search(&self, value: &T) -> std::result::Result<usize, usize> {
        let mut size = self.slice.len() / std::mem::size_of::<T>();
        if size == 0 {
            return Err(0);
        }
        let mut base = 0usize;

        // Note that the count of ids is not greater than `u16::MAX`. The number
        // of binary search steps will not over 16 unless the data is corrupted.
        // Let's relex to 20.
        let mut steps = 20usize;

        while steps > 0 && size > 1 {
            let half = size / 2;
            let mid = base + half;
            let cmp = unsafe { self.get_unchecked(mid) }.cmp(value);
            base = if cmp == Greater { base } else { mid };
            size -= half;
            steps -= 1;
        }

        let cmp = unsafe { self.get_unchecked(base) }.cmp(value);
        if cmp == Equal {
            Ok(base)
        } else {
            Err(base + (cmp == Less) as usize)
        }
    }
}