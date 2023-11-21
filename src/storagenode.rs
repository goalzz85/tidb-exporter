use std::path::PathBuf;
use crate::{errors::Error, tidbtypes::{DBInfo, TableInfo}, tabledataiterator::TableDataIterator, is_debug_mode};
use std::collections::HashMap;
use rocksdb::{DB, Options, DBIterator, BlockBasedOptions};
use txn_types::{WriteRef, Key, WriteType};
pub struct RocksDbStorageNode {
    db : DB
}

impl RocksDbStorageNode {
    pub fn new(db_path : &str) -> RocksDbStorageNode {
        let db_path_buf = PathBuf::from(db_path); 
        let mut opts = Options::default();
        opts.set_advise_random_on_open(false);
        opts.set_skip_checking_sst_file_sizes_on_db_open(true);
        opts.set_skip_stats_update_on_db_open(true);

        let mut bopts = BlockBasedOptions::default();
        bopts.disable_cache();
        opts.set_block_based_table_factory(&bopts);
        
        let cfs = ["default", "raft", "write", "lock"];
        let db = DB::open_cf_for_read_only(&opts, &db_path_buf, cfs.into_iter(), false).unwrap();
        
        //XXX handle the errors

        RocksDbStorageNode{
            db: db
        }
    }

    pub fn get_table_data_iter<'a, 'b>(&'b self, table_info : &'a TableInfo) -> Result<TableDataIterator<'a, 'b>, Error> {
        let default_cf_iter = match self.get_rocksdb_iter_by_cf_name(table_info.id, "default") {
            Ok(i) => i,
            Err(e) => return Err(Error::StorageNodeError(e.to_string())),
        };
        let write_cf_iter = match self.get_rocksdb_iter_by_cf_name(table_info.id, "write") {
            Ok(i) => i,
            Err(e) => return Err(Error::StorageNodeError(e.to_string())),
        };

        return Ok(TableDataIterator::new(table_info, default_cf_iter, write_cf_iter));
    }

    fn get_rocksdb_iter_by_cf_name<'a> (&'a self, table_id : i64, cf_name : &str) -> Result<DBIterator<'a>, Error> {
        let (lower_bound, upper_bound) = Self::get_table_data_keys_by_table_id(table_id);
        
        let mut readopts = rocksdb::ReadOptions::default();
        //4MB
        readopts.set_readahead_size(4 * 1024 * 1024);

        readopts.set_iterate_lower_bound(lower_bound);
        readopts.set_iterate_upper_bound(upper_bound);

        let cf = match self.db.cf_handle(cf_name) {
            Some(cf) => cf,
            None => return Err(Error::StorageNodeError(format!("cf {} not exists.", cf_name))),
        };

        let iter = self.db.iterator_cf_opt(cf, readopts, rocksdb::IteratorMode::Start);
        return Ok(iter);
    }

    pub fn get_databases(&self) -> Result<Vec<DBInfo>, Error>{
        // Keys used to scan database records
        let (lower_bound, upper_bound) = ([b'z', b'm', b'D', b'B', b's'], [b'z', b'm', b'D', b'B', b's', 0xff]);
        
        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_iterate_lower_bound(lower_bound);
        readopts.set_iterate_upper_bound(upper_bound);

        let default_cf = match self.db.cf_handle("write") {
            Some(cf) => cf,
            None => return Err(Error::StorageNodeError("cf write not exists.".to_string())),
        };
        
        let iter = self.db.iterator_cf_opt(&default_cf, readopts, rocksdb::IteratorMode::Start);
        let mut ret : Vec<DBInfo> = vec![];
        let mut cur_user_key : Vec<u8> = vec![];

        for item_res in iter {
            let user_key = match Key::truncate_ts_for(item_res.as_ref().unwrap().0.as_ref()) {
                Ok(k) => k,
                Err(e) => return Err(Error::CorruptedData(e.to_string())),
            };

            if cur_user_key.eq(user_key) {
                continue;
            }

            let write_ref = match WriteRef::parse(item_res.as_ref().unwrap().1.as_ref()) {
                Ok(r) => r,
                Err(_) => continue,
            };

            match write_ref.write_type {
                WriteType::Lock | WriteType::Rollback => continue,
                _ => ()
            };

            cur_user_key = user_key.to_vec();
            if write_ref.write_type.eq(&WriteType::Delete) {
                continue;
            }
            let db_info_res : DBInfo = match serde_json::from_slice(write_ref.short_value.unwrap()) {
                Ok(r) => r,
                Err(_) => {
                    if is_debug_mode() {
                        print!("database parse error, original data:\n{}", std::str::from_utf8(write_ref.short_value.unwrap()).unwrap())
                    };
                    continue;
                },
            };
            
            ret.push(db_info_res);
        }

        return Ok(ret);
    }


    pub fn get_table_info_by_dbid(&self, db_id : i64) -> Result<Vec<TableInfo>, Error> {
        let (start_key, end_key) = Self::get_table_info_keys_by_db_id(db_id);
        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_iterate_lower_bound(start_key);
        readopts.set_iterate_upper_bound(end_key);

        let default_cf = match self.db.cf_handle("default") {
            Some(cf) => cf,
            None => return Err(Error::StorageNodeError("cf default not exists.".to_string())),
        };

        let iter = self.db.iterator_cf_opt(&default_cf, readopts, rocksdb::IteratorMode::Start);
        let mut table_id_hash : HashMap<i64, TableInfo> = HashMap::new();
        let mut table_id_deleted_time_hash : HashMap<i64, i64> = HashMap::new();

        for item_res in iter {
            let v_data = item_res.as_ref().unwrap().1.as_ref();
            print!("table original data:\n{}\n", std::str::from_utf8(v_data).unwrap());
            let table_info : TableInfo = match serde_json::from_slice(v_data) {
                Ok(r) => r,
                Err(_) => {
                    if is_debug_mode() {
                        print!("table original data:\n{}\n", std::str::from_utf8(v_data).unwrap())
                    }
                    return Err(Error::CorruptedData("parse table info error".to_string()));
                },
            };

            if table_info.state != crate::tidbtypes::StatePublic {
                if table_info.state == crate::tidbtypes::StateDeleteOnly {
                    table_id_deleted_time_hash.entry(table_info.id).and_modify(|old_upt_ts| {
                        if *old_upt_ts < table_info.update_timestamp {
                            *old_upt_ts = table_info.update_timestamp;
                        }
                    }).or_insert(table_info.update_timestamp);
                }
                continue;
            }

            if let Some(old_table_info) = table_id_hash.get_mut(&table_info.id) {
                if old_table_info.update_timestamp < table_info.update_timestamp {
                    *old_table_info = table_info;
                }
            } else {
                table_id_hash.insert(table_info.id, table_info);
            }
        }

        for (table_id, delete_time) in table_id_deleted_time_hash {
            if let Some(entry) = table_id_hash.get(&table_id) {
                if entry.update_timestamp <= delete_time {
                    table_id_hash.remove(&table_id);
                }
            }
        }

        Ok(table_id_hash.into_values().collect())

    }

    fn get_table_info_keys_by_db_id(db_id :i64) -> (Vec<u8>, Vec<u8>) {
        let end_db_id = db_id + 1;
        let mut start_key = "mDB:".as_bytes().to_vec();
        let mut end_key = start_key.clone();
        let mut db_id_bytes_vec = db_id.to_string().as_bytes().to_vec();
        let mut end_db_id_bytes_vec = end_db_id.to_string().as_bytes().to_vec();
        start_key.append(&mut db_id_bytes_vec);
        end_key.append(&mut end_db_id_bytes_vec);
        let encoded_start_key = keys::data_key(tikv_util::codec::bytes::encode_bytes(&start_key).as_ref());
        let encoded_end_key = keys::data_key(tikv_util::codec::bytes::encode_bytes(&end_key).as_ref());
       
        return (encoded_start_key,  encoded_end_key);
    }

    fn get_table_data_keys_by_table_id(table_id : i64 ) -> (Vec<u8>, Vec<u8>) {
        let table_id_key_lower_bound = tidb_query_datatype::codec::table::encode_row_key(table_id, 0);
        let table_id_key_upper_bound = tidb_query_datatype::codec::table::encode_row_key(table_id, i64::MAX);
        let key_lower_bound = keys::data_key(txn_types::Key::from_raw(table_id_key_lower_bound.as_ref()).as_encoded());
        let key_upper_bound =  keys::data_key(txn_types::Key::from_raw(table_id_key_upper_bound.as_ref()).as_encoded());

        return (key_lower_bound, key_upper_bound);
    }
}