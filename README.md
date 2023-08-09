# pgworkload - workload utility for the PostgreSQL protocol

## Overview

The goal of `pgworkload` is to ease the creation of workload scripts by providing a utility with the most common functionality already implemented.

`pgworkload` is run in conjunction with a user supplied Python `class`. This class defines the workload transactions and flow.

The user has complete control of what statements the transactions actually execute, and what transactions are executed in which order.

`pgworkload` can seed a database with random generated data, whose definition is supplied in a YAML file.

A .sql file can be supplied to create the schema and run any special queries, eg. Zone Configuration changes.

## Example

Class `Bank` in file `workloads/bank.py` is an example of one such user-created workload.
The class defines 3 simple transactions that have to be executed by `pgworkload`.
Have a look at the `bank.py`, `bank.yaml` and `bank.sql` in the `workload` folder in this project.

Head to file `workload/bank.sql` to see what the database schema look like. We have 2 tables:

- the `transactions` table, where we record the bank payment transactions.
- the `ref_data` table.

Take a close look at this last table: each column represent a different type, which brings us to the next file.

File `bank.yaml` is the data generation definition file.
For each column of table `ref_data`, we deterministically generate random data.
This file is meant as a guide to show what type of data can be generated, and what args are required.

File `bank.py` defines the workload.
The workload is defined as a class object.
The class defines 3 methods: `init()`, `run()` and the constructor, `__init__()`.
All other methods are part of the application logic of the workload.
Read the comments along the code for more information.

Let's run the sample **Bank** workload.

### Step 0 - Create Python env

```bash
# upgrade pip - must have pip version 20.3+ 
pip3 install --upgrade pip

# we're now inside our virtual env
pip3 install pgworkload

# download the bank workload files
mkdir workloads
# the workload class
wget -P workloads https://raw.githubusercontent.com/fabiog1901/pgworkload/main/workloads/bank.py
# the schema file
wget -P workloads https://raw.githubusercontent.com/fabiog1901/pgworkload/main/workloads/bank.sql
# the data generation definition file
wget -P workloads https://raw.githubusercontent.com/fabiog1901/pgworkload/main/workloads/bank.yaml
# the arguments to pass at runtime, optional
wget -P workloads https://raw.githubusercontent.com/fabiog1901/pgworkload/main/workloads/bank.args.yaml
```

### Step 1 - Init the workload

We assume that your **CockroachDB** cluster or **PostgreSQL** server is up and running.

Init the **Bank** workload.

```bash
# CockroachDB
pgworkload init -w workloads/bank.py --url 'postgres://root@localhost:26257/postgres?sslmode=disable'

# PostgreSQL
pgworkload init --w workloads/bank.py --url 'postgres://root@localhost:5432/postgres?sslmode=disable'
```

You should see something like below

```text
2022-01-28 17:21:47,335 [INFO] (MainProcess 29422) URL: 'postgres://root@localhost:[5432|26257]/defaultdb?sslmode=disable&application_name=Bank'
2022-01-28 17:21:47,480 [INFO] (MainProcess 29422) Database 'bank' created.
2022-01-28 17:21:47,769 [INFO] (MainProcess 29422) Created workload schema
2022-01-28 17:21:47,789 [INFO] (MainProcess 29422) Generating dataset for table 'ref_data'
2022-01-28 17:22:07,088 [INFO] (MainProcess 29422) Importing data for table 'ref_data'
2022-01-28 17:22:21,063 [INFO] (MainProcess 29422) Init completed. Please update your database connection url to 'postgres://root@localhost:26257/bank?sslmode=disable&application_name=Bank'
```

`pgworkload` has read file `bank.sql` and has created the database and its schema.
It has then read file `bank.yaml` and has generated the CSV files for the table `ref_data`.
Finally, it imports the CSV files into database `bank`.

### Step 2 - Run the workload

Run the workload using 8 connections for 120 seconds or 100k cycles, whichever comes first.

```bash
# CockroachDB
pgworkload run -w workloads/bank.py -c 8 --url 'postgres://root@localhost:26257/bank?sslmode=disable&application_name=Bank' -d 120 -i 100000

# PostgreSQL
pgworkload run -w workloads/bank.py -c 8 --url 'postgres://root@localhost:5432/bank?sslmode=disable&application_name=Bank' -d 120 -i 100000
```

`pgworkload` uses exclusively the excellent [Psycopg 3](https://www.psycopg.org/psycopg3/docs/) to connect.
No other ORMs or drivers/libraries are used.
Psycopg has a very simple, neat way to [create connections and execute statements](https://www.psycopg.org/psycopg3/docs/basic/usage.html) and [transactions](https://www.psycopg.org/psycopg3/docs/basic/transactions.html).

`pgworkload` will output something like below

```text
2022-01-28 17:22:43,893 [INFO] (MainProcess 29511) URL: 'postgres://root@localhost:26257/bank?sslmode=disable&application_name=Bank'
id               elapsed    tot_ops    tot_ops/s    period_ops    period_ops/s    mean(ms)    p50(ms)    p90(ms)    p95(ms)    p99(ms)    pMax(ms)
-------------  ---------  ---------  -----------  ------------  --------------  ----------  ---------  ---------  ---------  ---------  ----------
__cycle__             10       1342       133.72          1342           134.2       54.9       35.76     165.94     192.89     245.42      333.6
read                  10       1215       121.03          1215           121.5       41.11      19.58     113.21     146.79     208.86      291.02
txn1_new              10        130        12.95           130            13         48.29      53.81      74.7       90.84      95.66      108.37
txn2_verify           10        129        12.85           129            12.9       70.9       73.73      94.3       99.69     137.99      164.96
txn3_finalize         10        127        12.65           127            12.7       67.21      72.48      93.64     105.97     129.57      166 

[...]

2022-01-28 17:24:44,765 [INFO] (MainProcess 29511) Requested iteration/duration limit reached. Printing final stats
id               elapsed    tot_ops    tot_ops/s    period_ops    period_ops/s    mean(ms)    p50(ms)    p90(ms)    p95(ms)    p99(ms)    pMax(ms)
-------------  ---------  ---------  -----------  ------------  --------------  ----------  ---------  ---------  ---------  ---------  ----------
__cycle__            121      14519       120.12            66             6.6       94.08      96.68     203.74     216.83     242.24      262.69
read                 121      13050       107.96            54             5.4       70.6       62.7      127.88     151.29     203.52      203.62
txn1_new             121       1469        12.15             7             0.7       51.08      51.07      71.71      73.66      75.23       75.62
txn2_verify          121       1469        12.15            11             1.1       70.52      76.92     102.31     102.32     102.33      102.33
txn3_finalize        121       1469        12.15            12             1.2       81.19      98.97     103.88     103.97     103.98      103.98 
```

There are many built-in options.
Check them out with

```bash
pgworkload --help
```

## Concurrency - processes and threads

pgworkload uses both the `multiprocessing` and `threading` library to achieve high concurrency, that is, opening multiple connections to the DBMS.

There are 2 parameters that can be used to configure how many processes you want to create, and for each process, how many threads:

- `--procs`, or `-x`, to configure the count of processes (defaults to the CPU count)
- `--concurrency`, or `-c`, to configure the total number of executing workloads to run (also referred to as _executing threads_)

pgworkload will spread the load across the processes, so that each process has an even amount of threads.

Example: if we set `--procs 4` and `--concurrency 10`, pgworkload will create as follows:

- Process-1: MainThread + 2 extra threads. Total = 3
- Process-2: MainThread + 2 extra threads. Total = 3
- Process-3: MainThread + 1 extra thread.  Total = 2
- Process-3: MainThread + 1 extra thread.  Total = 2

Total workloads = 10

This allows you to fine tune the count of Python processes and threads to fit your system.

## Built-in Workloads

`pgworkload` has the following workload already built-in and can be called without the need to pass a class file

### Querybench

Querybench runs a list of SQL Statements sequentially and iteratively.
It assumes the schema and data have been created and loaded.

SQL statements file `mystmts.sql`

```sql
-- Query 1
select 1;
select 
  version();
-- select now();

-- Query 2
SELECT * FROM my_table 
WHERE id = 1234;
```

Run **Querybench** like this:

```bash
pgworkload run --builtin-workload Querybench --args mystmts.sql --url <conn-string>
```

### Hovr

Coming soon

## Acknowledgments

Some methods and classes have been taken and modified from, or inspired by, <https://github.com/cockroachdb/movr>
