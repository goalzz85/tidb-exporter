
mod errors;
mod storagenode;
mod tidbtypes;
mod tabledataiterator;
mod datum;
mod writer;

use clap::Parser;

use crate::{storagenode::RocksDbStorageNode, writer::{TiDBExportWriter, csvwriter::CsvWriter}, tidbtypes::TableInfo};

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
    #[arg(short, long)]
    table : Option<String>,

    ///the writer that the data will be written to. only support 'csv' for now.
    #[arg(short, long, required_unless_present_all(["database", "table"]), value_names(["csv"]))]
    writer : Option<String>,

    #[arg(short, long, required_if_eq("writer", "csv"))]
    export : Option<String>,

    ///compressing exported files by gzip or not.
    #[arg(short, long, default_value_t = false)]
    gzip : bool,

    ///maximum size of each exported file is measured in MiB. file will be sperated into multiple files with file size smaller than the file_size you have set.
    #[arg(short, long, default_value_t = 0)]
    file_size : usize,
}

fn main() {
    let cli = Cli::parse();
    let rocksdb_node = RocksDbStorageNode::new(&cli.path);


    if cli.database.is_none() {
        print_databases(&rocksdb_node);
        return;
    }

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

    let mut export_writer : Box<dyn TiDBExportWriter>;
    if cli.writer.unwrap().eq("csv") {
        export_writer = Box::new(CsvWriter::new(table_info_opt.unwrap(), cli.export.unwrap().as_str(), cli.file_size, cli.gzip));
    } else {
        print!("not supoort writer!");
        return;
    }

    for table_info in table_infos {
        export_data(&rocksdb_node, table_info, export_writer.as_mut());
    }
    print!("Hello World!");
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


fn export_data(rocksdb_node : &RocksDbStorageNode, table_info : &TableInfo, export_writer : &mut dyn TiDBExportWriter) {
    let data_iterator = match rocksdb_node.get_table_data_iter(table_info) {
        Ok(i) => i,
        Err(..) => {
            print!("get data iterator error.");
            return;
        }
    };

    for row_data in data_iterator {
        match export_writer.write_row_data(row_data) {
            Ok(_) => continue,
            Err(e) => {
                print!("{:?}\n", e);
            }
        }
    }
    export_writer.flush();
}