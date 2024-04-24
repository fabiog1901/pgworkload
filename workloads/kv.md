# KV workload

The KV workload allows you great flexibility when choosing the column types and sizes for both the key and the value columns.

Available data types are `bytes` (default), `int`, `uuid`, `string`.

For types `bytes` and `string`, you can optionally choose the payload size; it defaults to 32 for they key and 256 for the value.

You can configure the `batch_size` (defaults to 1) as well as the `cycle_size` (defaults to 1).
The `batch_size` configures the size of the multi-row `INSERT` statement, while `cycle_size` configures the size of one `pgworkload` iteration cycle.

You can also fine-tune your read/write/update/delete ratio using the `read_pct`, `update_pct` and `delete_pct` arguments, and have the SELECTs executed as historical (`AOST`) queries with argument `aost`.

Configuring a large `key_pool_size` allows your read to be more random, but the pool will take longer to fill with keys you have inserted, and it will consume more memory.
Conversely, a smaller pool is cheaper on resources and fills up quicklier, but there will be fewer keys to pick from.
The pool is a fixed size [`deque` object](https://docs.python.org/3/library/collections.html#collections.deque): as you insert new keys into the table, the keys are added to the deque.
When you have added more keys than the pre-defined size, older keys are pushed out and only the most recent keys are kept.
During a read operation, a random value from the deque is picked as the predicate for the SELECT/UPDATE/DELETE statement.

You are not bound to a simple Key and Value workload: you can elect to have multiple columns as your Primary Key, aka a composite key, and multiple columns for your values.

## Args

Here are the avaliable **arguments** to pass at runtime:

| prop          | description                                                    | default |
| ------------- | -------------------------------------------------------------- | ------- |
| think_time    | wait time between execution in ms                              | 10      |
| batch_size    | size of the multi-row INSERT/UPSERT                            | 1       |
| cycle_size    | size of the pgworkload iteration                               | 1       |
| table_name    | name of the table to insert into                               | kv      |
| key_types     | key types comma separated list                                 | bytes   |
|               | types: bytes, uuid, int, string, fixed                         |         |
| key_sizes     | key sizes bytes and string types only) comma separated list    | 32      |
| value_types   | value types comma separated list                               | bytes   |
|               | types: bytes, uuid, int, string, fixed                         |         |
| value_sizes   | value sizes (bytes and string types only) comma separated list | 256     |
| fixed_value   | hardcoded value for type `fixed`                               |         |
| read_pct      | the percent of operations that are SELECT statements           | 0       |
| update_pct    | the percent of operations that are UPDATE statements           | 0       |
| delete_pct    | the percent of operations that are DELETE statements           | 0       |
| key_pool_size | the size of the list to pick keys from for read operation      | 10000   |
| write_mode    | `insert`, `upsert`, `do_nothing` for `ON CONFLICT DO NOTHING`  | insert  |
| aost          | value to pass to the `AS OF SYSTEM TIME` clause.               |         |  
|               | set to `fr` for _follower_read_timestamp()_                    |         |

## Examples

### Exemple 1 - basic usage

Run a KV workload against a table `kv_int_str` that uses `INT` as key and `STRING` as value of 50 chars, in batches of 16.

First, you need to create your target table.
The KV workload will not create the schema for you.

```sql
CREATE TABLE kv_int_str (k INT8 PRIMARY KEY, v STRING);
```

Then run `pgworkload` using these args:

```bash
pgworkload run [...] \
  --args '{"key_types":"int", "value_types":"string", "value_sizes":50, "batch_size":16, "table_name":"kv_int_str"}'
```

And this is the sample data inserted

```sql
> select * from kv_int_str limit 2;                                                                          
         k         |                         v
-------------------+-----------------------------------------------------
   514985589634278 | bVXvph5AzVtaHzbpdbteRE9QWAExcB3qbmiY1a4FFhgFIGDeyy
  1384392319947339 | c0pOG44hvdLMxJR85hSHJvcnhwBhGOLGaDzgxZBcgUSadCuyza
(2 rows)
```

### Example 2 - multiple value columns

Run a KV workload with 3 value columns (STRING, BYTES and INT) with a mix of selects (80%), updates (10%) and upserts.

Please note, the KV workload expects the key column to be called `k` and the first of the value columns to be called `v`.
Any additional column might be called with any name.

Also, the UPDATE command only updates the `v` column.

```sql
CREATE TABLE k3v (
    k UUID NOT NULL, -- the key column must be called `k`
    v STRING NULL, -- the first value column must be called `v`
    v1 BYTES NULL,
    v2 INT8 NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT now():::TIMESTAMPTZ ON UPDATE now():::TIMESTAMPTZ,
    CONSTRAINT pk PRIMARY KEY (k ASC)
);
```

Then run `pgworkload` using these args - notice the final comma in `value_sizes`: you need to make sure that you have matching _types_ and _sizes_.

```bash
pgworkload run [...] \
  --args '{"key_types":"uuid", "value_types":"string,bytes,int", "value_sizes":"10,16,", "table_name":"k3v", "write_mode":"upsert", "read_pct":80, "update_pct":10}'
```

Here's the output after 100 iterations: see the ops distribution in the `tot_ops` column

```text
2024-04-23 11:15:46,534 [INFO] (MainProcess 9006 MainThread) run:186 : Requested iteration/duration limit reached. Printing final stats
id           elapsed    tot_ops    tot_ops/s    period_ops    period_ops/s    mean(ms)    p50(ms)    p90(ms)    p95(ms)    p99(ms)    pMax(ms)
---------  ---------  ---------  -----------  ------------  --------------  ----------  ---------  ---------  ---------  ---------  ----------
__cycle__          2        100        47              100            10         16.81      13.12      33.12      36.81      42.31       63.61
__think__          2        100        46.98           100            10         11.05      10.87      12.45      12.54      12.58       12.59
read_kv            2         84        39.45            84             8.4        2.82       1.91       2.53       2.7       24.54       53.31
update_kv          2          8         3.76             8             0.8       19.56      24.37      26.36      28.04      29.39       29.72
write_kv           2          8         3.76             8             0.8       22.24      21.23      24.4       26.39      27.98       28.38 
```

We executed 8 upserts:

```sql
> select * from k3v;                                                                                         
                   k                   |     v      |                 v1                 |         v2          |              ts
---------------------------------------+------------+------------------------------------+---------------------+--------------------------------
  09a7e34c-483a-46d8-93b7-e7c2c4a94c94 | W5OKys4lDA | \x7da116b278f1b9a5fb12535f68243752 | 2992646263282535785 | 2024-04-23 15:15:46.475319+00
  0b20c37b-f3ec-4f45-80a1-d1a69a327d69 | 6DPGjjcJrk | \x9ff379dee54432eec789ea886eecea94 | 2944760581257279808 | 2024-04-23 15:15:45.973856+00
  0d6f09e6-7a9c-4ed9-9f90-53f5dff5a36a | etnUAQCMnh | \x8740b3a35845e9cf4976eda52c3d76ec |  732001563447105408 | 2024-04-23 15:15:46.310024+00
  3dca925d-30df-451a-a570-9ce1d0c34f9b | aMKdCVUxIo | \x51b216f474ae5c85239f18b36a4e5355 | 2272284303391115106 | 2024-04-23 15:15:45.331872+00
  533773df-beff-4ab6-b367-561e551106cc | ks3dIZjwzX | \x805ac602adf86a71548d9188873efa0d | 4090105512421217003 | 2024-04-23 15:15:45.479441+00
  53874334-0e1e-4be2-8f72-f93ffb117970 | ogmJ5newSC | \x8b462daeee657f0a19809eaa0e31653b |  794358578559101311 | 2024-04-23 15:15:45.510974+00
  8c29ab71-00b5-4e26-a474-ec2c313d27f5 | dgL3akOzVN | \x823e36c5da639b76beb4435fc969c255 | 7197843808776724295 | 2024-04-23 15:15:45.725499+00
  8c8ebb32-72a7-4005-9c4a-8dc98b2917f4 | SpBaDamnlA | \xdcb7c5c797f5bdc69c71e27b4e27b0ff | 3046560917210849958 | 2024-04-23 15:15:46.182559+00
(8 rows)
```

### Example 3 - composite key and multiple value columns

Run a KV workload on a composite key, with a mix of DELETEs and AOST queries.

```sql
CREATE TABLE compkv (
    k UUID NOT NULL,
    k1 STRING NOT NULL, -- key columns must follow the pattern `k`+ 1, 2, 3, ...
    v STRING NULL,
    v1 BYTES NULL,
    v2 INT8 NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT now():::TIMESTAMPTZ ON UPDATE now():::TIMESTAMPTZ,
    CONSTRAINT pk PRIMARY KEY (k ASC, k1 ASC) -- notice it's a composite key
);
```

As in the previous example, notice the comma in the `key_sizes`.

```bash
pgworkload run [...] \
  --args '{"table_name":"compkv", "key_types":"uuid,string", "key_sizes":",3", "value_types":"string,bytes,int", "value_sizes":"5,1,", "delete_pct": 10, "read_pct": 20, "aost":"-1s"}'
```

After 100 iterations

```text
id           elapsed    tot_ops    tot_ops/s    period_ops    period_ops/s    mean(ms)    p50(ms)    p90(ms)    p95(ms)    p99(ms)    pMax(ms)
---------  ---------  ---------  -----------  ------------  --------------  ----------  ---------  ---------  ---------  ---------  ----------
__cycle__          3        100        30.83           100            10         28.25      30.62      32.68      36.12      55.87       66.8
__think__          3        100        30.82           100            10         11.35      11.23      12.54      12.56      12.59       12.59
delete_kv          3          5         1.54             5             0.5       20.99      20.56      22.42      22.97      23.4        23.51
read_kv            3         19         5.85            19             1.9        1.87       1.7        2.58       2.69       2.92        2.98
write_kv           3         76        23.41            76             7.6       20.33      19.37      20.74      27.54      46.2        54.19
```

We executed 76 inserts and 5 deletes.
The total shows we have 71 rows in our table.

```sql
> select * from compkv;
                   k                   | k1  |   v   |  v1  |         v2          |              ts
---------------------------------------+-----+-------+------+---------------------+--------------------------------
  015e8d0f-f334-42cc-ba96-4be9ed1b5bde | ywm | Gs9cZ | \xc0 | 4875346769445311596 | 2024-04-23 18:16:12.684904+00
  [...]
  f8173d3b-f287-4c88-98bc-dc68f606cc6e | qvy | jMdim | \xae | 6115621406519874297 | 2024-04-23 18:16:13.279258+00
  fc599e29-c157-4b5b-a26f-f311096094c8 | 9R3 | OpvqD | \xc5 | 5290408709402028339 | 2024-04-23 18:16:12.359142+00
(71 rows)
```

### Example 4 - partitioning by list

Run a KV workload against a table that is partitioned geographically.

```sql
CREATE TABLE kv_loc (
    k STRING NOT NULL,
    k1 STRING NOT NULL,
    v BYTES NULL,
    CONSTRAINT pk PRIMARY KEY (k ASC, k1 ASC)
) PARTITION BY LIST (k) (
   PARTITION west VALUES IN (('WEST')),
   PARTITION east VALUES IN (('EAST'))
);
```

Then you can use the `fixed` type and the `fixed_value` argument to simulate 2 distinct flows:

```bash
# EAST FLOW
pgworkload run [...] \
  --args '{"key_types":"fixed,string", "key_sizes":",10", "table_name":"kv_loc", "fixed_value":"EAST"}'```
```

And on another server

```bash
# WEST FLOW
pgworkload run [...] \
  --args '{"key_types":"fixed,string", "key_sizes":",10", "table_name":"kv_loc", "fixed_value":"WEST"}'```
```

Here's some rows

```sql
> select k, k1 from kv_loc limit 5;
   k   |     k1
-------+-------------
  EAST | 1EfCgiWsp2
  EAST | 4MW1PwYciw
  EAST | 4fHOBeVBJZ
  EAST | 582RdAw0WV
  EAST | 5ePVKECiLI
(5 rows)
```
