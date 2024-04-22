# KV workload

The KV workload allows you great flexibility when choosing the column types and sizes for both the key and the value column.

Available data types are `bytes` (default), `int`, `uuid`, `string`.

For types `bytes` and `string`, you can optionally choose the payload size; it defaults to 32 for they key and 256 for the value.

You can configure the `batch_size` (defaults to 1) as well as the `cycle_size` (defaults to 100).
The `batch_size` configures the size of the multi-value `INSERT` statement, while `cycle_size` configures the size of one `pgworkload` iteration cycle.

## Args

Here are the avaliable **arguments** to pass at runtime:

| prop       | description                                    | default |
| ---------- | ---------------------------------------------- | ------- |
| think_time | wait time between execution in ms              | 10      |
| batch_size | size of the multi-value INSERT                 | 1       |
| cycle_size | size of the pgworkload iteration               | 100     |
| table_name | name of the table to insert into               | kv      |
| key_type   | data type (bytes, uuid, int, string)           | bytes   |
| key_size   | size of the key (bytes and string type only)   | 32      |
| value_type | data type (bytes, uuid, int, string)           | bytes   |
| key_size   | size of the value (bytes and string type only) | 256     |

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
