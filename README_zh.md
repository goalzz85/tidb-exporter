# tidb-exporter

TiDB使用RocksDB作为底层存储引擎（实际上是TiKV在使用）。`tidb-exporter`可以直接从RocksDB数据文件中直接导出TiDB数据，甚至是在TiDB无法正常工作情况下。这是很救命的，特别是在TiDB挂了，又启动不起来的时候。

**使用的时候一定要确保TiDB或TiKV服务没有工作的时候，只支持v5.x以上版本。**

![tidb-exporter](assets/tidb-exporter.png)

# 列出数据库

只使用'`-p`'参数将列出服务节点中所有的数据库列表。

```bash
./tidb-exporter -p /data/tikv/db
```
```
1, test
10216, user
11455, xxl_job
11506, product
11558, task
```
第一行是数据库内部序号，由TiDB内部创建。第二行是数据库名。

# 列出表格

除'`-p`' 参数外，你额外设置'`-d`'参数指定某个数据库，将列出指定数据库中的所有数据库表。

```bash
./tidb-exporter -p /data/tikv/db -d user
```
```
8980, user_avatar
11906, user_detail
```

# 导出数据

当使用'`-t`'参数指定需要导出的数据库表，需要同时指定'`-e`'参数指定`Exporter`，虽然当前只支持`csv`。最后使用'`-w`'参数指定导出文件的写入地址。

```bash
./tidb-exporter -p /data/tikv/db -d user -t user_avatar -e csv -w ~/user.csv
```

tidb-exporter 导出指定数据库表在RocksDB中的所有数据，也就是会导出所有的`region`，就算它在该节点中并不是`leader`。如果你有一个包含三个节点的集群，理论上会导出这个数据库表的所有数据。

这也是个学习`Rust`的练手项目，内存管理思路确实和其它语言差距很大，个人感觉虽然麻烦，拉高了使用门槛，但也拉高了代码质量下限，对工程和长期可维护性提供了更好的保障。