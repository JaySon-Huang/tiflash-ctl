# TiFlash-ctl (prototype)

This is a prototype for TiFlash Controller - a command line tool for TiFlash Server

* * *

## Build

* `make` to build `tiflash-ctl`

## Usage

* Run `./bin/tiflash-ctl --help` to check the usage
* Subcommand `check`: some troubleshooting tools for TiFlash
* Subcommand `dispatch`: dispatch debug function for TiFlash Server

## Command description
### `check consistency`
#### 作用描述及注意事项
部分 tiflash 的 bug 会导致 tiflash 上的数据与 tikv 的数据不一致。此命令可以检查出哪些 Region 在 tiflash 上的 peer 与 tikv 之间存在 **行数不一致** 的问题。
如果 tiflash 节点中存在与 tikv 之间不一致数据的 peer，程序最终会列出通过 `pd-ctl` 清理那些 tiflash Region peer 的命令。

通过 `pd-ctl` 执行上述命令清理后，再次运行 `check consistency` 程序，验证不一致问题是否得到修复。如果问题仍存在，需要再次清理不一致的 Region peer。

> 注意:
> 1. 该程序只适用于使用 int-like 类型的列做主键的表（或者没有定义主键，默认使用 `_tidb_rowid` 作为主键的表也可以使用）。不适用于使用非 int 类型或者多列组成 clustered_index 的表。
> 2. 暂时不适用于开启了 TLS 的集群
> 3. 在 PD 执行 remove 有问题的 tiflash Region peer 后，需要一定的时间让 tiflash 重新通过 apply snapshot 的方式从 tikv 同步数据，期间可能导致查询有些抖动。
> 4. 预期最多清理两次后，数据不一致问题会被修复

#### 参数说明
```
Usage:
  tiflash-ctl check consistency [flags]

Flags:
      # 常用的参数
      --database string          The database name of query table
      --table string             The table name of query table
      --tidb_ip string           A TiDB instance IP (default "127.0.0.1")
      --tidb_port int32          The port of TiDB instance (default 4000)
      --user string              TiDB user (default "root")
      --password string          TiDB user password
      # 根据该表建了多少个 tiflash 副本指定，默认值为 2
      --num_replica int          The number of TiFlash replica for the query table (default 2)
      # 对于使用 int-like 类型的列作为主键的表，通过此参数指定列的名字
      --row_id_col_name string   The TiDB row id column name (default "_tidb_rowid")
      # 用于辅助定位主键范围的参数，一般不需要设置
      --lower_bound int          The lower bound of query (leave it to be default)
      --upper_bound int          The upper bound of query (leave it to be default)
```
#### 操作步骤

步骤 1，查询哪些 tiflash Region peer 存在数据不一致的情况：
根据参数说明指定查询的 tidb 以及发现不一致问题的表，执行 `check consistency` 命令并将运行结果重定向至文件。如：
```bash
# Run the check and find the region peers we need to remove from the log
> ./tiflash-ctl check consistency --database test --table test_table --tidb_ip ${TIDB_IP} --tidb_port ${TIDB_PORT} > check.log

# The output will show the region with different number of rows between tikv and tiflash
> cat check.log
# ...
# select count(*) from `test`.`test_table` where 2432113 <= _tidb_rowid and _tidb_rowid < 3238283 => 4ms (tiflash)
# Range [2432113, 3238283), num of rows: tikv 0, tiflash 863. FAIL
# Region {581 7480000000000000FF435F728000000000FF251C710000000000FA 7480000000000000FF435F728000000000FF31698B0000000000FA [{582 5 Voter} {583 62 Learner} {584 95 Learner}]} have not consist num of rows
# operator add remove-peer 581 62
# operator add remove-peer 581 95
# ...

# Get the region peers we need to remove 
> grep 'operator' check.log
operator add remove-peer 581 62
operator add remove-peer 581 95
operator add remove-peer 699 63
operator add remove-peer 699 64
operator add remove-peer 829 63
operator add remove-peer 829 64
```

步骤 2，通过 `pd-ctl` 连接至集群，执行上述命令将存在不一致数据的 tiflash Region peer 移除。移除后，PD 自动会挑选新的 tiflash 节点补全数据。
```bash
# Remove the region peers that contains incorrect data, PD will schedule new peers on TiFlash and overwrite the incorrect data automatically
> tiup ctl:v5.4.0 pd -u ${PD_IP}:${PD_PORT} -i
» operator add remove-peer 581 62
Success!
» operator add remove-peer 581 95
Success!
» operator add remove-peer 699 63
Success!
» operator add remove-peer 699 64
Success!
» operator add remove-peer 829 63
Success!
» operator add remove-peer 829 64
Success!
```

步骤 3，再次检查，确认数据不一致情况是否得到解决。正常的表，tikv 和 tiflash 的 RowID range，以及表中记录的行数应该一致。如：
```bash
# repeat to check and run again if need
> ./bin/tiflash-ctl check consistency --database test --table test_table --tidb_ip ${TIDB_IP} --tidb_port ${TIDB_PORT} > check_2.log
# The row id range and number of rows of tikv and tiflash shown in the output file should be the same
> cat check_2.log
set tidb_allow_batch_cop = 0 => 4ms
set tidb_allow_mpp = 0 => 0ms
select min(_tidb_rowid), max(_tidb_rowid) from `test`.`test_table` => 24036ms (tikv)
select min(_tidb_rowid), max(_tidb_rowid) from `test`.`test_table` => 557ms (tiflash)
RowID range: [62530067, 64156203] (tikv)           <-- check the rowid range
RowID range: [62530067, 64156203] (tiflash)        <-- check the rowid range
Init query ranges: [[62530067, 64156204)]
select count(*) from `test`.`test_table` where 62530067 <= _tidb_rowid and _tidb_rowid < 64156204 => 488ms (tikv)
select count(*) from `test`.`test_table` where 62530067 <= _tidb_rowid and _tidb_rowid < 64156204 => 173ms (tiflash)
select count(*) from `test`.`test_table` where 62530067 <= _tidb_rowid and _tidb_rowid < 64156204 => 2ms (tikv)
select count(*) from `test`.`test_table` where 62530067 <= _tidb_rowid and _tidb_rowid < 64156204 => 186ms (tiflash)
Range [62530067, 64156204), num of rows: tikv 1624960, tiflash 1624960. OK   <-- check the number of rows
```
