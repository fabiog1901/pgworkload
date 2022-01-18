# pgWorkload - workload framework for the PostgreSQL protocol

## Overview

The goal of pgWorkload is to ease the creation of workload scripts by providing a framework with the most common functionality already implemented.

File `pgWorkload.py` is run in conjunction with a user supplied Python `class`. This class defines the workload transactions and flow.

The user has complete control of what statements the transactions actually execute, and what transactions are executed in which order.

pgWorkload can seed a database with random generated data, whose definition is supplied in a YAML file.

A .sql file can be supplied to create the schema and run any special queries, eg. Zone Configuration changes.

## Example

Class `Bank` in file `workloads/bank.py` is an example of one such user-created workload.
The class defines 3 simple transactions that have to be executed by `pgWorkload.py`.

Let's run the sample **Bank** workload.

### Step 0 - Create python env

Let's create a Virtual environment for our testing

```bash
python3 -m venv venv
cd venv
source bin/activate

# now we're inside our virtual env
pip3 install psycopg psycopg-binary numpy tabulate pandas pyyaml

# clone this repo
git clone https://github.com/fabiog1901/pgWorkload
cd pgWorkload
```

Just to confirm:

```bash
(venv) $ python3 -V
Python 3.9.9

(venv) $ pip3 -V
pip 21.2.4 from /Users/fabio/pgWorkload/venv/lib/python3.9/site-packages/pip (python 3.9)

(venv) $ pip3 freeze
numpy==1.22.1
pandas==1.3.5
psycopg==3.0.8
psycopg-binary==3.0.8
python-dateutil==2.8.2
pytz==2021.3
PyYAML==6.0
six==1.16.0
tabulate==0.8.9
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
python3 pgWorkload.py --workload=workloads/bank.py --concurrency=8 --parameters 50 wire --init
```

You should see something like below

```text
2022-01-18 17:09:07,730 [INFO] (MainProcess 24211) dburl: 'postgres://root@localhost:26257/defaultdb?sslmode=disable&application_name=Bank'
2022-01-18 17:09:07,908 [INFO] (MainProcess 24211) Database 'bank' created.
2022-01-18 17:09:08,172 [INFO] (MainProcess 24211) Created workload schema
2022-01-18 17:09:08,196 [INFO] (MainProcess 24211) Generating dataset for table 'ref_data'
2022-01-18 17:09:54,555 [INFO] (MainProcess 24211) Init completed. Please update your database connection url to 'postgres://root@localhost:26257/bank?sslmode=disable&application_name=Bank'
```

### Step 2 - Run the workload

Run the workload using 8 connections for 120 seconds or 100k cycles, whichever comes first.

```bash
python3 pgWorkload.py --workload=workloads/bank.py --concurrency=8 --parameters 90 wire --url='postgres://root@localhost:26257/bank?sslmode=disable&application_name=Bank' --duration=120 --iterations=100000
```

pgWorkload will output something like below

```text
2022-01-18 17:11:24,539 [INFO] (MainProcess 24450) dburl: 'postgres://root@localhost:26257/bank?sslmode=disable&application_name=Bank'
id               elapsed    tot_ops    tot_ops/s    period_ops    period_ops/s    mean(ms)    p50(ms)    p90(ms)    p95(ms)    p99(ms)    pMax(ms)
-------------  ---------  ---------  -----------  ------------  --------------  ----------  ---------  ---------  ---------  ---------  ----------
__cycle__             10       4537       452.12          4537           453.7       16.28       0.45      98.12     112.19     152.16      214.85
read                  10       4073       405.76          4073           407.3        4.47       0.42      36.32      37.98      43.2        94.28
txn1_new              10        466        46.41           466            46.6       37.44      35.64      52.34      65.58      73.65       83.29
txn2_verify           10        466        46.41           466            46.6       41.84      38.06      55.85      73.19      93.56       94.47
txn3_finalize         10        464        46.21           464            46.4       40.42      38.19      43.32      56.1       74.36       94.35 

[...]


2022-01-18 17:13:25,294 [INFO] (MainProcess 24450) Requested iteration/duration limit reached. Printing final stats
id               elapsed    tot_ops    tot_ops/s    period_ops    period_ops/s    mean(ms)    p50(ms)    p90(ms)    p95(ms)    p99(ms)    pMax(ms)
-------------  ---------  ---------  -----------  ------------  --------------  ----------  ---------  ---------  ---------  ---------  ----------
__cycle__            121      59924       496.24           297            29.7       17.09       0.57     102.33     115.76     147.83      149.62
read                 121      53951       446.78           265            26.5        4.51       0.52       0.93      38.52      55.9        57.38
txn1_new             121       5973        49.46            26             2.6       34.27      33.19      38.02      50.63      55.55       55.82
txn2_verify          121       5973        49.46            30             3         42.21      38.82      56.03      56.08      56.11       56.12
```

## Acknowledgments

Some methods and classes have been taken and modified from, or inspired by, <https://github.com/cockroachdb/movr>
