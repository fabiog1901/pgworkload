**pgworkload has matured and became [dbworkload](https://github.com/fabiog1901/dbworkload)!**

# pgworkload - workload utility for the PostgreSQL protocol

## Overview

The goal of `pgworkload` is to ease the creation of workload scripts by providing a utility with the most common functionality already implemented.

`pgworkload` is run in conjunction with a user supplied Python `class`. This class defines the workload transactions and flow.

The user has complete control of what statements the transactions actually execute, and what transactions are executed in which order.

`pgworkload` can seed a database with random generated data, whose definition is supplied in a YAML file and can be extracted from a DDL SQL file.

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
The class defines 2 methods: `run()` and the constructor, `__init__()`.
All other methods are part of the application logic of the workload.
Read the comments along the code for more information.

Let's run the sample **Bank** workload.

### Step 0 - env setup

```bash
# upgrade pip - must have pip version 20.3+ 
pip3 install --upgrade pip

pip3 install pgworkload

mkdir workloads
cd workloads

# the workload class
wget https://raw.githubusercontent.com/fabiog1901/pgworkload/main/workloads/bank.py

# the DDL file
wget https://raw.githubusercontent.com/fabiog1901/pgworkload/main/workloads/bank.sql

# the data generation definition file
wget https://raw.githubusercontent.com/fabiog1901/pgworkload/main/workloads/bank.yaml
```

### Step 1 - init the workload

Make sure your **CockroachDB** cluster or **PostgreSQL** server is up and running.

Connect to the SQL prompt and execute the DDL statements in the `bank.sql` file.
In CockroachDB, you can simply run

```sql
sql> \i bank.sql
```

Next, generate some CSV data to seed the database:

```bash
pgworkload util csv -i bank.yaml -x 1
```

The CSV files will be located inside a `bank` directory.

```bash
$ ls -lh bank
total 1032
-rw-r--r--  1 fabio  staff   513K Apr  9 13:01 ref_data.0_0_0.csv

$ head -n2 bank/ref_data.0_0_0.csv 
0       simplefaker     b66ab5dc-1fcc-4ac8-8ad0-70bbbb463f00    alpha   16381   {124216.6,416559.9,355271.42,443666.45,689859.03,461510.94,31766.46,727918.45,361202.5,561364.1}        12421672576.9632        2022-10-18 04:57:37.613512      2022-10-18     13:36:48 1001010011      \xe38a2e10b400a8e77eda  {ID-cUJeNcMZ,ID-mWxhyiqN,ID-0FnlVOO5}   0       "{""k"":""cUJNcMZ""}"
1       simplefaker     f2ebb78a-5af3-4755-8c22-2ad06aa3b26c    bravo   39080           35527177861.6551        2022-12-25 09:12:04.771673      2022-12-25      13:05:42        0110111101      \x5a2efedf253aa3fbeea8  {ID-gQkRkMxIkSjihWcWTcr,ID-o7iDzl9AMJoFfduo6Hz,ID-5BS3MlZgOjxFZRBgBmf}  0       "{""k"":""5Di0UHLWMEuR7""}"
```

Now you can import the CSV file.
In CockroachDB, my favorite method is to use a webserver to serve the CSV file.
Open a new terminal then start a simple python server

```bash
cd workloads
cd bank
python3 -m http.server 3000
```

If you open your browser at <http://localhost:3000> you should see file `ref_data.0_0_0.csv` being served.

At the SQL prompt, import the file

```sql
sql> IMPORT INTO ref_data CSV DATA ('http://localhost:3000/ref_data.0_0_0.csv') WITH delimiter = e'\t'; 
```

In PostgreSQL Server, at the SQL prompt, just use `COPY`

```sql
bank=# COPY ref_data FROM '/Users/fabio/workloads/bank/ref_data.0_0_0.csv' WITH CSV DELIMITER AS e'\t';
COPY 100
Time: 2.713 ms
```

### Step 2 - Run the workload

Run the workload using 4 connections for 120 seconds or 100k cycles, whichever comes first.

```bash
# CockroachDB
pgworkload run -w bank.py -c 4 --url 'postgres://root@localhost:26257/bank?sslmode=disable&application_name=Bank' -d 120 -i 100000

# PostgreSQL
pgworkload run -w bank.py -c 4 --url 'postgres://fabio:postgres@localhost:5432/bank?sslmode=disable&application_name=Bank' -d 120 -i 100000
```

`pgworkload` uses exclusively the excellent [Psycopg 3](https://www.psycopg.org/psycopg3/docs/) to connect.
No other ORMs or drivers/libraries are used.
Psycopg has a very simple, neat way to [create connections and execute statements](https://www.psycopg.org/psycopg3/docs/basic/usage.html) and [transactions](https://www.psycopg.org/psycopg3/docs/basic/transactions.html).

`pgworkload` will output rolling statistics about throughput and latency for each transaction in your workload class

```text
id               elapsed    tot_ops    tot_ops/s    period_ops    period_ops/s    mean(ms)    p50(ms)    p90(ms)    p95(ms)    p99(ms)    pMax(ms)
-------------  ---------  ---------  -----------  ------------  --------------  ----------  ---------  ---------  ---------  ---------  ----------
__cycle__             10       3206       320.13          3206           320.6       11.4        0.49      23.31      23.97      25.13       54.63
read                  10       1614       161.15          1614           161.4        0.19       0.17       0.28       0.31       0.4         1.82
txn1_new              10       1596       159.34          1596           159.6        0.34       0.29       0.44       0.49       0.67       21.13
txn2_verify           10       1594       159.13          1594           159.4       11.15      10.88      11.93      12.5       13.23       43.28
txn3_finalize         10       1592       158.92          1592           159.2       11.23      10.94      12.02      12.68      13.37       39.77 

[...]

2024-05-17 15:28:11,589 [INFO] (MainProcess MainThread) run:194: Requested iteration/duration limit reached. Printing final stats
id               elapsed    tot_ops    tot_ops/s    period_ops    period_ops/s    mean(ms)    p50(ms)    p90(ms)    p95(ms)    p99(ms)    pMax(ms)
-------------  ---------  ---------  -----------  ------------  --------------  ----------  ---------  ---------  ---------  ---------  ----------
__cycle__            121      40639       336.43           251            25.1       11.55       0.44      24.1       24.9       25.99       26.38
read                 121      20427       169.1            128            12.8        0.22       0.19       0.33       0.35       0.41        0.44
txn1_new             121      20212       167.32           120            12          0.38       0.34       0.5        0.58       0.76        0.79
txn2_verify          121      20212       167.32           121            12.1       11.38      11.17      12.65      12.93      13.38       13.39
txn3_finalize        121      20212       167.32           123            12.3       11.54      11.41      12.73      13.03      13.55       13.97
```

You can always use **pgAdmin** for PostgreSQL Server or the **DB Console** for CockroachDB to view your workload, too.

![pg_admin](media/pg_admin.png)

There are many built-in options.
Check them out with

```bash
pgworkload --help
```

## How it works

It’s helpful to understand first what `pgworkload` does:

- At runtime, `pgworkload` first imports the class you pass, `bank.py`.
- It spawns _n_ threads for concurrent execution (see next section on Concurrency).
- By default, it sets the connection to `autocommit` mode.
- **psycopg v3** will _PREPARE_ statements automatically after 5 executions.
- Each thread creates a database connection - no need for a connection pool.
- In a loop, each `pgworkload` thread will:
  - execute function `run()` which returns a list of functions.
  - execute each function in the list sequentially. Each function, typically, executes a SQL statement/transaction.
- Execution stats are funneled back to the _MainThread_, which aggregates and prints them to _stdout_.
- If the connection drops, it will recreate it. You can also program how long you want the connection to last.
- `pgworkload` stops once a limit has been reached (iteration/duration), or you Ctrl+C.

## Concurrency - processes and threads

`pgworkload` uses both the `multiprocessing` and `threading` library to achieve high concurrency, that is, opening multiple connections to the DBMS.

There are 2 parameters that can be used to configure how many processes you want to create, and for each process, how many threads:

- `--procs/-x`, to configure the count of processes (defaults to the CPU count)
- `--concurrency/-c`, to configure the total number of connections (also referred to as _executing threads_)

`pgworkload` will spread the load across the processes, so that each process has an even amount of threads.

Example: if we set `--procs 4` and `--concurrency 10`, pgworkload will create as follows:

- Process-1: MainThread + 1 extra threads. Total = 2
- Process-2: MainThread + 1 extra threads. Total = 2
- Process-3: MainThread + 2 extra thread.  Total = 3
- Process-4: MainThread + 2 extra thread.  Total = 3

Total executing threads/connections = 10

This allows you to fine tune the count of Python processes and threads to fit your system.

Furthermore, each _executing thread_ receives a unique ID (an integer).
The ID is passed to the workload class with function `setup()`, along with the total count of threads, i.e. the value passed to `-c/--concurrency`.
You can leverage the ID and the thread count in various ways, for example, to have each thread process a subset of a dataset.

## Generating CSV files

- You can seed a database quickly by letting `pgworkload` generate pseudo-random data and import it.
- `pgworkload` takes the DDL as an input and creates an intermediate YAML file, with the definition of what data you want to create (a string, a number, a date, a bool..) based on the column data type.
- You then refine the YAML file to suit your needs, for example, the size of the string, a range for a date, the precision for a decimal, a choice among a discrete list of values..
- You can also specify what is the percentage of NULL for any column, or how many elements in an ARRAY type.
- You then specify the total row count, how many rows per file, and in what order, if any, to sort by.
- Then `pgworkload` will generate the data into CSV or TSV files, compress them if so requested.
- You can then optionally merge-sort the files using command `merge`.

Write up blog: [Generate multiple large sorted csv files with pseudo-random data](https://dev.to/cockroachlabs/generate-multiple-large-sorted-csv-files-with-pseudo-random-data-1jo4)

Find out more on the `yaml`, `csv` and `merge` commands by running

```bash
pgworkload util --help
```

Consult file `workloads/bank.yaml` for a list of all available generators and options.

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

## Acknowledgments

Some methods and classes have been taken and modified from, or inspired by, <https://github.com/cockroachdb/movr>
