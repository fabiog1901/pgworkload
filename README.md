# pgWorkload - workload framework for the PostgreSQL protocol

WARNING: This is still a work-in-progress project.

## Overview

The goal of pgWorkload is to ease the creation of workload scripts by providing a framework with the most common functionality already implemented.

File `pgWorkload.py` is run in conjunction with a user supplied Python `class`. This class defines the workload transactions and flow.

The user has complete control of what statements the transactions actually execute, and what transactions are executed in which order.

## Example

Class `Bank` in file `workloads/bank.py` is an example of one such user-created workload.
The class defines 3 simple transactions that have to be executed by `pgWorkload.py`.

Let's run the sample **Bank** workload.

### Step 0 - Create python env

Let's create a Virtual environment for our testing

```bash
python3 -m venv venv
source venv/bin/activate

# now we're inside our virtual env
pip3 install psycopg psycopg-binary numpy tabulate pandas pyyaml
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

Init the **Bank** workload

```bash
python3 pgWorkload.py --workload=workloads/bank.py --concurrency=8 --parameters 50 wire --init
```

### Step 2 - Run the workload

Run the workload using 8 connections for 120 seconds.

```bash
python3 pgWorkload.py --workload=workloads/bank.py --concurrency=8 --parameters 90 wire --url='postgres://root@localhost:26257/bank?sslmode=disable&application_name=Bank' --duration=120
```

pgWorkload will output something like below

```text
2022-01-18 16:42:41,241 [INFO] (MainProcess 22029) dburl: 'postgres://root@localhost:26257/bank?sslmode=disable&application_name=Bank'
id               elapsed    tot_ops    tot_ops/s    period_ops    period_ops/s    mean(ms)    p50(ms)    p90(ms)    p95(ms)    p99(ms)    pMax(ms)
-------------  ---------  ---------  -----------  ------------  --------------  ----------  ---------  ---------  ---------  ---------  ----------
__cycle__             10       4714       469.77          4714           471.4       15.55       0.46      75.63     111.8      136.39      175.99
read                  10       4246       422.97          4246           424.6        4.45       0.44       2.24      37.97      43.12       96.23
txn1_new              10        473        47.11           473            47.3       35.51      35.37      41.38      53.66      69.97       74.6
txn2_verify           10        471        46.9            471            47.1       40.24      38.11      43.32      56.19      74.67       96.38
txn3_finalize         10        468        46.6            468            46.8       40.19      38.05      42.82      56.37      74.87       96.2 

[...]

2022-01-18 16:44:42,080 [INFO] (MainProcess 22029) Requested iteration/duration limit reached. Printing final stats
id               elapsed    tot_ops    tot_ops/s    period_ops    period_ops/s    mean(ms)    p50(ms)    p90(ms)    p95(ms)    p99(ms)    pMax(ms)
-------------  ---------  ---------  -----------  ------------  --------------  ----------  ---------  ---------  ---------  ---------  ----------
__cycle__            121      61000       504.8            285            28.5       19.74       0.57     106.88     113.4      148.33      152.3
read                 121      55033       455.42           248            24.8        5.21       0.52      37.33      38.08      50.08       57.79
txn1_new             121       5967        49.38            32             3.2       37         36.11      50.79      56.69      70.5        76.21
txn2_verify          121       5967        49.38            36             3.6       40.33      38.12      49.71      56.67      58.14       58.15
txn3_finalize        121       5967        49.38            37             3.7       40.08      37.9       46.37      58.02      60.42       61.74 
```

## Acknowledgments

Some methods and classes have been taken and modified from, or inspired by, <https://github.com/cockroachdb/movr>
