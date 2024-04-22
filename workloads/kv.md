# KV workload

The KV workload allows you great flexibility when choosing the column types and sizes for both the key and the value column.

Available data types are `bytes` (default), `int`, `uuid`, `string`.

For types `bytes` and `string`, you can optionally choose the payload size; it defaults to 32 for they key and 256 for the value.

You can configure the `batch_size` (defaults to 1) as well as the `cycle_size` (defaults to 100).
The `batch_size` configures the size of the multi-row `INSERT` statement, while `cycle_size` configures the size of one `pgworkload` iteration cycle.

You can also fine-tune your read/write ratio using the `read_pct` argument.
Configuring a large `key_pool_size` allows your read to be more random, but the pool will take longer to fill with keys you have inserted, and it will consume more memory.
Conversely, a smaller pool is cheaper on resources and fills up quicklier, but there will be fewer keys to pick from.

The pool is a fixed size [`deque` object](https://docs.python.org/3/library/collections.html#collections.deque): as you insert new keys into the table, the keys are added to the deque.
When you have added more keys than the pre-defined size, older keys are pushed out and only the most recent keys are kept.
During a read operation, a random value from the deque is picked as the predicate for the SELECT statement.

## Args

Here are the avaliable **arguments** to pass at runtime:

| prop          | description                                                    | default |
| ------------- | -------------------------------------------------------------- | ------- |
| think_time    | wait time between execution in ms                              | 10      |
| batch_size    | size of the multi-row INSERT                                   | 1       |
| cycle_size    | size of the pgworkload iteration                               | 100     |
| table_name    | name of the table to insert into                               | kv      |
| key_type      | data type (bytes, uuid, int, string)                           | bytes   |
| key_size      | size of the key (bytes and string type only)                   | 32      |
| value_type    | data type (bytes, uuid, int, string)                           | bytes   |
| key_size      | size of the value (bytes and string type only)                 | 256     |
| ~~seed~~      | ~~the random generator seed number~~  COMING SOON!             |         |
| read_pct      | The percent of operations that are SELECT statements           | 0       |
| key_pool_size | The size of the list to pick keys from for read operation      | 10000   |
| write_mode    | `insert`, `update`, `do_nothing` for `ON CONFLICT DO NOTHING`  | insert  |

## Example

Run a KV workload against a table `kv_int_str` that uses `INT` as key and `STRING` as value of 50 chars, in batches of 16.

```sql
CREATE TABLE kv_int_str (k INT8 PRIMARY KEY, v STRING);
```

Then run `pgworkload` using these args:

```bash
pgworkload run [...] \
  --args '{"key_type":"int", "value_type":"string", "value_size":50, "batch_size":16, "table_name":"kv_int_str"}'
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
