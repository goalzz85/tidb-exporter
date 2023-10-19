#![allow(non_upper_case_globals, non_camel_case_types, non_snake_case)]
#![allow(unused_variables, dead_code)]

use serde::{Deserialize, Deserializer};
use tidb_query_datatype::{FieldTypeTp, FieldTypeFlag};

#[derive(Deserialize, Debug, Clone)]
pub struct CIStr {
    pub O: String,
    pub L: String
}

pub type SchemaState = u8;

//from tidb definition
pub const StateNone : SchemaState = 0;
pub const StateDeleteOnly : SchemaState = 1;
pub const StateWriteOnly : SchemaState = 2;
pub const StateWriteReorganization : SchemaState = 3;
pub const StateDeleteReorganization : SchemaState = 4;
pub const StatePublic : SchemaState = 5;
pub const StateReplicaOnly : SchemaState = 6;
pub const StateGlobalTxnOnly : SchemaState = 7;

pub type IndexType = i32;
pub const IndexTypeInvalid : SchemaState = 0;
pub const IndexTypeBtree : SchemaState = 1;
pub const IndexTypeHash : SchemaState = 2;
pub const IndexTypeRtree : SchemaState = 3;

//from tidb definition
#[derive(Deserialize, Debug)]
pub struct DBInfo {
    pub id: i64,
    pub db_name: CIStr,
    pub charset: String,
    pub collate: String,
    pub state: SchemaState
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableInfo {
    pub id: i64,
    pub name: CIStr,
    pub charset: String,
    pub collate: String,
    pub cols : Vec<ColumnInfo>,
    #[serde(deserialize_with = "deserialize_null_default")]
    pub index_info : Vec<IndexInfo>,
    pub state : SchemaState,
    pub pk_is_handle : bool,
    pub is_common_handle : bool,
    pub common_handle_version : u16,
    pub comment : String,
    pub auto_inc_id : i64,
    pub auto_id_cache : i64,
    pub update_timestamp : i64,
    pub version : u16,
    pub partition : Option<PartitionInfo>,
}

impl TableInfo {
    pub fn have_partitions(&self) -> bool {
        return self.partition.is_some();
    }

    pub fn get_partiton_table_infos(&self) -> Vec<TableInfo> {
        if self.partition.is_none() {
            return Vec::default();
        }

        let partition_info = self.partition.as_ref().unwrap();

        let mut partition_table_infos : Vec<TableInfo> = Vec::with_capacity(partition_info.definitions.len());
        for partition in partition_info.definitions.iter() {
            let mut partition_table_info = self.clone();
            partition_table_info.partition = None;
            partition_table_info.id = partition.id;
            partition_table_infos.push(partition_table_info);
        }

        return partition_table_infos;
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PartitionInfo {
    pub definitions : Vec<PartitionDefinition>,
}

#[derive(Debug, Clone,Deserialize)]
pub struct PartitionDefinition {
    pub id : i64,
    pub name : CIStr,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FieldType {
    pub Tp : u8,
    pub Flag : u32,
    pub Flen : u32,
    pub Decimal : i32,
    pub Charset : String,
    pub Collate : String,
    
    #[serde(deserialize_with = "deserialize_null_default")]
    pub Elems : Vec<String>,
}

impl FieldType {
    pub fn is_hybrid(&self) -> bool {
        let tp = FieldTypeTp::from_u8(self.Tp).unwrap_or(FieldTypeTp::Unspecified);
        return tp == FieldTypeTp::Enum || tp == FieldTypeTp::Set || tp == FieldTypeTp::Bit;
    }


    pub fn is_unsigned(&self) -> bool {
        return self.Flag & FieldTypeFlag::UNSIGNED.bits() != 0;
    }

    pub fn has_prikey_flag(&self) -> bool {
        return (self.Flag & 1 << 1) > 0;
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnInfo {
    pub id: i64,
    pub name : CIStr,
    pub offset : i32,
    
    #[serde(rename = "type")]
    pub field_type: FieldType,
    pub state : SchemaState,
    pub comment : String,
    pub hidden : bool,
    pub version : u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndexColumn {
    name : CIStr,
    offset : i32,
    length : i32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndexInfo {
    id : i64,
    idx_name : CIStr,
    tbl_name : CIStr,
    idx_cols : Vec<IndexColumn>,
    state : SchemaState,
    comment : String,
    index_type : IndexType,
    is_unique : bool,
    is_primary : bool,
    is_invisible : bool,
    is_global : bool,
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: Default + Deserialize<'de>,
    D: Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}