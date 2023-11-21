
mod errors;
mod storagenode;
mod tidbtypes;
mod tabledataiterator;
mod datum;
mod export;

use std::{sync::{Arc, RwLock}, thread};

use lazy_static::lazy_static;
use clap::{Parser, builder::ArgPredicate};
use export::{exporter::TiDBExporter, CsvExporter};


use crate::{storagenode::RocksDbStorageNode, tidbtypes::TableInfo};

lazy_static! {
    static ref IS_DEBUG_MODE: RwLock<bool> = RwLock::new(false);
}

fn set_debug_mode(value: bool) {
    let mut write = IS_DEBUG_MODE.write().unwrap();
    *write = value;
}

fn is_debug_mode() -> bool {
    *IS_DEBUG_MODE.read().unwrap()
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    ///rocksdb directory path of using by TiDB.
    #[arg(short, long)]
    path : String,

    ///database name for listing tables or exporting.
    #[arg(short, long)]
    database : Option<String>,

    ///table name that need to be exported, must be in the database specified by --database
    #[arg(short, long, requires_if(ArgPredicate::IsPresent, "exporter"))]
    table : Option<String>,

    ///the exporter that the data will be written to. only support 'csv' for now.
    #[arg(short, long, value_names(["csv"]))]
    exporter : Option<String>,

    #[arg(short = 'w', long, required_if_eq("exporter", "csv"))]
    write_path : Option<String>,

    ///compressing exported files by gzip or not.
    #[arg(short, long, default_value_t = false)]
    gzip : bool,

    ///maximum size of a single file, and is measured in MB. the file will be splitted into multiple files depend on the internal write buffer size (default: 50MB) and whether gzip compression is enabled.
    #[arg(short = 's', long, default_value_t = 0)]
    file_size : usize,

    ///number of threads for concurrent exporting.
    #[arg(short = 'n', long, default_value_t = 3)]
    thread_num : usize,

    ///display debug messages.
    #[arg(long, default_value_t = false)]
    debug : bool
}

fn main() {
    let cli = Cli::parse();
    let rocksdb_node = RocksDbStorageNode::new(&cli.path);


    if cli.database.is_none() {
        print_databases(&rocksdb_node);
        return;
    }
    set_debug_mode(cli.debug);

    let dbs = match rocksdb_node.get_databases() {
        Ok(d) => d,
        Err(e) => {
            print!("{:?}", e);
            return;
        }
    };

    let database_name = cli.database.as_ref().unwrap();
    let db_info_opt = dbs.iter().find(|&d| d.db_name.L.eq(database_name));
    if db_info_opt.is_none() {
        print!("not fount database: {}\n", database_name);
        print_databases(&rocksdb_node);
        return;
    }


    if cli.table.is_none() {
        print_tables(&rocksdb_node, db_info_opt.unwrap().id);
        return;
    }

    let db_id = db_info_opt.unwrap().id;
    let tables = match rocksdb_node.get_table_info_by_dbid(db_id) {
        Ok(t) => t,
        Err(e) => {
            print!("{:?}", e);
            return;
        }
    };

    let table_name = cli.table.as_ref().unwrap();
    let table_info_opt = tables.iter().find(|&t| t.name.L.eq(table_name));
    if table_info_opt.is_none() {
        print!("not fount table: {}\n", table_name);
        print_tables(&rocksdb_node, db_id);
        return;
    }

    let original_table_info = table_info_opt.unwrap();
    let mut table_infos : Vec<&TableInfo> = Vec::new();
    let partition_table_infos : Vec<TableInfo>;
    if original_table_info.have_partitions() {
        partition_table_infos = original_table_info.get_partiton_table_infos();
        table_infos = partition_table_infos.iter().collect();
    } else {
        table_infos.push(original_table_info);
    }

    let rn_arc = Arc::new(rocksdb_node);
    for table_info in table_infos {
        export_data(rn_arc.clone(), table_info, &cli);
    }
}


fn print_databases(rocksdb_node : &RocksDbStorageNode) {
    let db_info_vec = rocksdb_node.get_databases().unwrap();
    for db_info in db_info_vec {
        print!("{}, {}\n", db_info.id, db_info.db_name.L);
    }
}

fn print_tables(rocksdb_node : &RocksDbStorageNode, db_id : i64) {
    let table_info_vec = rocksdb_node.get_table_info_by_dbid(db_id).unwrap();
    for table_info in table_info_vec {
        print!("{}, {}\n", table_info.id, table_info.name.L);
    }
}


fn export_data(rocksdb_node : Arc<RocksDbStorageNode>, table_info : &TableInfo, cli : &Cli) {
    let (tx, rx) = crossbeam_channel::bounded(10);
    let transmitter_handler;
    {
        let rd_node = rocksdb_node.clone();
        let table_clone = table_info.clone();
        transmitter_handler = thread::spawn(move || {
            if let Ok(data_iterator) = rd_node.get_table_data_iter(&table_clone) {
                let rows_block_size : usize = 100;

                let mut rows_block = Vec::with_capacity(rows_block_size);

                for row_data in data_iterator {
                    rows_block.push(row_data);
                    if rows_block.len() == rows_block_size {
                        tx.send( rows_block).unwrap();
                        rows_block = Vec::with_capacity(rows_block_size);
                    }
                }
                if !rows_block.is_empty() {
                    tx.send( rows_block).unwrap();
                }
            } else {
                panic!("get data iterator failed");
            }
            drop(rd_node);
        });
    }

    let thread_num = cli.thread_num;
    let mut exporter = get_export_writer_by_cli(cli, table_info);
    exporter.set_thread_num(thread_num);

    let handlers = exporter.start_export(rx.clone());

    _ = transmitter_handler.join();
    for h in handlers {
        _ = h.join();
    }
    drop(rx);
}

fn get_export_writer_by_cli(cli : &Cli, table_info : &TableInfo) -> Box<dyn TiDBExporter> {
    let exporter_name = cli.exporter.clone().unwrap_or("csv".to_string());

    if exporter_name.eq("csv") {
        return Box::new(get_csv_exporter(cli, table_info));
    }

    panic!("exporter {} not exists.", exporter_name);
}

fn get_csv_exporter(cli : &Cli, table_info : &TableInfo) -> CsvExporter {
    let write_path = cli.write_path.clone().unwrap();
    let file_size_mb = cli.file_size;
    let is_gzip = cli.gzip;

    return CsvExporter::new(table_info.clone(), &write_path, file_size_mb, is_gzip);
}