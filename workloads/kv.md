# KV workload

The KV workload allows you great flexibility when choosing the column types and sizes for both the key and the value column.

Available data types are `bytes` (default), `int`, `uuid`, `string`.

For types `bytes` and `string`, you can optionally choose the payload size; it defaults to 32 for they key and 256 for the value.

You can configure the `batch_size` (defaults to 1) as well as the `cycle_size` (defaults to 100).
The `batch_size` configures the size of the multi-value `INSERT` statement, while `cycle_size` configures the size of 1 `pgworkload` iteration cycle.

Here are the avaliable **arguments** to pass at runtime:

| prop | description | default |
| -------- | ---------- | -- |
|      | Medium     |
| Markdown | High       |