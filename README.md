# pgworkload - workload framework for the PostgreSQL protocol

## Overview

The goal of **pgworkload** is to ease the creation of workload scripts by providing a framework with the most common functionality already implemented.

`pgworkload` is run in conjunction with a user supplied Python `class`. This class defines the workload transactions and flow.

The user has complete control of what statements the transactions actually execute, and what transactions are executed in which order.

pgworkload can seed a database with random generated data, whose definition is supplied in a YAML file.

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
This file is meant as a guide to show what type of data can be generated, and what parameters are required.

File `bank.py` defines the workload.
The workload is defined as a class object.
The class defines 3 methods: `init()`, `run()` and the constructor, `__init__()`.
All other methods are part of the application logic of the workload.
Read the comments along the code for more information.

Let's run the sample **Bank** workload.

### Step 0 - Create python env

Let's create a Virtual environment for our testing

```bash
python3 -m venv venv
cd venv
source bin/activate

# we're now inside our virtual env
pip3 install pgworkload

# clone this repo
git clone https://github.com/fabiog1901/pgworkload
cd pgworkload
```

Just to confirm:

```bash
(venv) $ python3 -V
Python 3.9.9

(venv) $ pip3 -V
pip 21.2.4 from /Users/fabio/pgworkload/venv/lib/python3.9/site-packages/pip (python 3.9)
```

### Step 1 - Create cluster and init the workload

For simplicity, we create a local single-node cluster.

Open a new Terminal window, and start the cluster and access the SQL prompt

```bash
cockroach start-single-node --insecure --background

cockroach sql --insecure
```

Back to the previous terminal, init the **Bank** workload

```bash
pgworkload --workload=workloads/bank.py --concurrency=8 --parameters 50 wire --init
```

You should see something like below

```text
2022-01-19 09:16:55,870 [INFO] (MainProcess 5194) URL: 'postgres://root@localhost:26257/defaultdb?sslmode=disable&application_name=Bank'
2022-01-19 09:16:56,042 [INFO] (MainProcess 5194) Database 'bank' created.
2022-01-19 09:16:56,324 [INFO] (MainProcess 5194) Created workload schema
2022-01-19 09:16:56,345 [INFO] (MainProcess 5194) Generating dataset for table 'ref_data'
2022-01-19 09:17:15,508 [INFO] (MainProcess 5194) Importing data for table 'ref_data'
2022-01-19 09:17:29,284 [INFO] (MainProcess 5194) Init completed. Please update your database connection url to 'postgres://root@localhost:26257/bank?sslmode=disable&application_name=Bank'
```

pgworkload has read file `bank.sql` and has created the database and its schema.
It has then read file `bank.yaml` and has generated the CSV files for the table `ref_data`.
Finally, it imports the CSV files into database `bank`.

### Step 2 - Run the workload

Run the workload using 8 connections for 120 seconds or 100k cycles, whichever comes first.

```bash
pgworkload --workload=workloads/bank.py --concurrency=8 --parameters 90 wire --url='postgres://root@localhost:26257/bank?sslmode=disable&application_name=Bank' --duration=120 --iterations=100000
```

pgworkload uses exclusively the excellent [Psycopg 3](https://www.psycopg.org/psycopg3/docs/) to connect.
No other ORMs or drivers/libraries are used.
Psycopg has a very simple, neat way to [create connections and execute statements](https://www.psycopg.org/psycopg3/docs/basic/usage.html) and [transactions](https://www.psycopg.org/psycopg3/docs/basic/transactions.html).

pgworkload will output something like below

```text
2022-01-19 09:18:04,679 [INFO] (MainProcess 5331) URL: 'postgres://root@localhost:26257/bank?sslmode=disable&application_name=Bank'
id               elapsed    tot_ops    tot_ops/s    period_ops    period_ops/s    mean(ms)    p50(ms)    p90(ms)    p95(ms)    p99(ms)    pMax(ms)
-------------  ---------  ---------  -----------  ------------  --------------  ----------  ---------  ---------  ---------  ---------  ----------
__cycle__             10       4775       475.93          4775           477.5       15.38       0.45      73.99     111.44     140.79      207.18
read                  10       4307       429.15          4307           430.7        4.32       0.42       1.06      37.9       43.71       95.76
txn1_new              10        473        47.12           473            47.3       36.35      35.15      44.94      55.4       73.43       94.69
txn2_verify           10        472        47.02           472            47.2       40.58      38.06      43.08      57.1       74.95       95.79
txn3_finalize         10        468        46.61           468            46.8       39.86      38.01      39.62      55.4       74.09       95.65 

[...]

2022-01-19 09:20:05,504 [INFO] (MainProcess 5331) Requested iteration/duration limit reached. Printing final stats
id               elapsed    tot_ops    tot_ops/s    period_ops    period_ops/s    mean(ms)    p50(ms)    p90(ms)    p95(ms)    p99(ms)    pMax(ms)
-------------  ---------  ---------  -----------  ------------  --------------  ----------  ---------  ---------  ---------  ---------  ----------
__cycle__            121      58989       488.22           423            42.3       12.75       0.52      38.43     110.03     143.61      155.38
read                 121      53005       438.69           389            38.9        3.57       0.5        0.85      37.79      41.99       73.51
txn1_new             121       5984        49.52            31             3.1       34.28      34.14      38.5       51.94      58.75       61
txn2_verify          121       5984        49.52            33             3.3       42.31      38.09      56.31      66.25      73.82       73.82
txn3_finalize        121       5984        49.52            34             3.4       39.98      37.96      42.98      57.89      61.23       61.25 
```

There are many built-in options.
Check them out with

```bash
pgworkload -h
```

## Acknowledgments

Some methods and classes have been taken and modified from, or inspired by, <https://github.com/cockroachdb/movr>
