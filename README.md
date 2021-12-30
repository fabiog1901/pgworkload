# pgWorkload - workload framework for the PostgreSQL protocol

WARNING: This is still a work-in-progress project.

## Overview

The goal of pgWorkload is to ease the creation of workloads scripts by providing a pseudo-framework with the most common functionality already implemented.

File `pgWorkload.py` is run in conjunction with a user supplied Python `class`. This class defines the workload transactions and flow.

The user has complete control of what statements the transactions actually execute, and what transactions are executed in which order.

## Example

Class `Bank` in file `workloads/samples.py` is an example of one such user-created workload.
The class defines 3 simple transactions that have to be executed by `pgWorkload.py`.

Let's run the sample **Bank** workload.

### Step 0 - Create python env

Let's create a Virtual environment for our testing

```bash
python3 -m venv venv
source venv/bin/activate

# now we're inside our virtual env
pip3 install psycopg psycopg-binary numpy tabulate
```

Just to confirm:

```bash
(venv) fabio@mac: ~/pgWorkload $ python3 -V
Python 3.9.9

(venv) fabio@mac: ~/pgWorkload $ pip3 -V
pip 21.2.4 from /Users/fabio/pgWorkload/venv/lib/python3.9/site-packages/pip (python 3.9)

(venv) fabio@mac: ~/pgWorkload $ pip3 freeze
numpy==1.21.5
psycopg==3.0.7
psycopg-binary==3.0.7
tabulate==0.8.9
```

### Step 1 - Create cluster and load schema

For simplicity, we create a local single-node cluster

```bash
cockroach start-single-node --insecure --background

cockroach sql --insecure
```

Create the simple schema in database `defaultdb`

```sql
CREATE TABLE bank (
    id UUID, 
    event INT, 
    lane STRING, 
    ts TIMESTAMP, 
    PRIMARY KEY (id, event)
);
```

### Step 2 - Run the workload

Run the workload using 2 connections for 120 seconds or 10k times, whichever comes first.

```bash
python3 pgWorkload.py --concurrency=2 --duration=120 --iterations=10000 --workload=workloads/samples.py --workload-class=Bank --parameters=swift
```

pgWorkload will output something like below

```text
2021-12-26 17:52:36,910 [INFO] (MainProcess 10373) dburl: 'postgres://root@localhost:26257/defaultdb?sslmode=disable&application_name=Bank'
transaction name      elapsed_time    total_ops    tot_ops/second    period_ops    period_ops/second    p50(ms)    p90(ms)    p95(ms)    max(ms)
------------------  --------------  -----------  ----------------  ------------  -------------------  ---------  ---------  ---------  ---------
__cycle__                       10          164             15.94           164                 16.4     110.79     131.62     149.65     223.4
txn0                            10          166             16.14           166                 16.6      36.54      38.99      50.92      80.82
txn1                            10          165             16.04           165                 16.5      36.77      42.01      56.14      77.73
txn2                            10          164             15.94           164                 16.4      36.81      41.04      54.03      74.56 

[...]
2021-12-26 17:54:37,610 [INFO] (MainProcess 10373) Requested iteration/duration limit reached. Printing final stats
transaction name      elapsed_time    total_ops    tot_ops/second    period_ops    period_ops/second    p50(ms)    p90(ms)    p95(ms)    max(ms)
------------------  --------------  -----------  ----------------  ------------  -------------------  ---------  ---------  ---------  ---------
__cycle__                      121         2037             16.88             8                  0.8     110.77     114.77     114.97     115.17
txn0                           121         2037             16.88             6                  0.6      36.04      36.62      36.71      36.79
txn1                           121         2037             16.88             8                  0.8      35.86      41.94      42         42.05
txn2                           121         2037             16.88             8                  0.8      36.39      38.04      39.32      40.59 
```

On the SQL terminal, the data has been inserted

```text
root@:26257/defaultdb> select * from bank limit 5;
                   id                  | event | lane  |             ts
---------------------------------------+-------+-------+-----------------------------
  00291758-3616-4cc9-b0b2-e8dbc10a266e |     0 | swift | 2021-12-26 17:57:31.644156
  00291758-3616-4cc9-b0b2-e8dbc10a266e |     1 | swift | 2021-12-26 17:57:31.679694
  00291758-3616-4cc9-b0b2-e8dbc10a266e |     2 | swift | 2021-12-26 17:57:31.717327
  00a4a750-90a5-4ab1-8779-324d34d4f706 |     0 | swift | 2021-12-26 17:57:21.626455
  00a4a750-90a5-4ab1-8779-324d34d4f706 |     1 | swift | 2021-12-26 17:57:21.662035
(5 rows)

Time: 1ms total (execution 1ms / network 0ms)
```

## Acknowledgments

Some methods and classes have been taken and modified from, or inspired by, <https://github.com/cockroachdb/movr>
