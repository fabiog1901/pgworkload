#!/usr/bin/python

import argparse
import datetime as dt
import json
import logging
import multiprocessing as mp
import os
import pgworkload.utils.simplefaker
import pgworkload.utils.util
import psycopg
import queue
import random
import re
import signal
import sys
import threading
import time
import traceback
import yaml

DEFAULT_SLEEP = 5


def signal_handler(sig, frame):
    """Handles Ctrl+C events gracefully, 
    ensuring all running processes are closed rather than killed.

    Args:
        sig (_type_): 
        frame (_type_): 
    """
    global stats
    global concurrency
    logging.info("KeyboardInterrupt signal detected. Stopping processes...")

    # send the poison pill to each worker
    for _ in range(concurrency):
        kill_q.put(None)

    # wait until all workers return
    start = time.time()
    c = 0
    timeout = True
    while c < concurrency and timeout:
        try:
            kill_q2.get(block=False)
            c += 1
        except:
            pass

        time.sleep(0.01)
        timeout = time.time() < start + 5

    if not timeout:
        logging.info("Timeout reached - forcing processes to stop")

    logging.info("Printing final stats")
    stats.print_stats()
    sys.exit(0)


def init_pgworkload(args: argparse.Namespace):
    """Performs pgworkload initialization steps

    Args:
        args (argparse.Namespace): args passed at the CLI

    Returns:
        argparse.Namespace: updated args
    """
    logging.debug("Initialazing pgworkload")

    if not args.procs:
        args.procs = os.cpu_count()

    if not re.search(r'.*://.*/(.*)\?', args.dburl):
        logging.error(
            "The connection string needs to point to a database. Example: postgres://root@localhost:26257/postgres?sslmode=disable")
        sys.exit(1)

    if not args.workload_path:
        logging.error("No workload argument was passed")
        print()
        args.parser.print_help()
        sys.exit(1)

    workload = pgworkload.utils.util.import_class_at_runtime(path=args.workload_path)

    args.dburl = pgworkload.utils.util.set_query_parameter(url=args.dburl, param_name="application_name",
                                                     param_value=args.app_name if args.app_name else workload.__name__)

    logging.info(f"URL: '{args.dburl}'")

    # load args dict from file or string
    if os.path.exists(args.args):
        with open(args.args, 'r') as f:
            args.args = f.read()
            # parse into JSON if it's a JSON string
            try:
                args.args = json.load(args.args)
            except Exception as e:
                pass
    else:
        args.args = yaml.safe_load(args.args)
        if isinstance(args.args, str):
            logging.error(
                f"The value passed to '--args' is not a valid JSON or a valid path to a JSON/YAML file: '{args.args}'")
            sys.exit(1)

    return args


def init(args: argparse.Namespace):
    """Initialize the workload.
    Includes tasks like:
    - create database and schema;
    - generate random datasets
    - import datasets into the database

    Args:
        args (argparse.Namespace): the args passed at the CLI
    """
    logging.debug("Running init")

    args = init_pgworkload(args)

    if not args.db:
        args.db = os.path.splitext(
            os.path.basename(args.workload_path))[0].lower()

    # PG or CRDB?
    try:
        dbms: str = pgworkload.utils.util.get_dbms(args.dburl)
    except ValueError as e:
        logging.error(e)
        sys.exit(1)
    except Exception as e:
        logging.error(e)
        dbms: str = None

    # PART 1 - CREATE THE SCHEMA
    if args.skip_schema:
        logging.debug("Skipping init_create_schema")
    else:
        __init_create_schema(args.dburl,
                             args.drop, args.db, args.workload_path, dbms)

    # PART 2 - GENERATE THE DATA
    if args.skip_gen:
        logging.debug("Skipping init_generate_data")
    else:
        __init_generate_data(
            args.procs, args.workload_path, dbms, args.csv_max_rows)

    # PART 3 - IMPORT THE DATA
    dburl = pgworkload.utils.util.get_new_dburl(args.dburl, args.db)
    if args.skip_import:
        logging.debug("Skipping init_import_data")
    else:
        if not args.http_server_hostname:
            args.http_server_hostname = pgworkload.utils.util.get_hostname()
            logging.debug(
                f"Hostname identified as: '{args.http_server_hostname}'")

        __init_import_data(dburl, args.workload_path, dbms,
                           args.http_server_hostname, args.http_server_port)

    # PART 4 - RUN WORKLOAD INIT
    logging.debug("Running workload.init()")
    workload = pgworkload.utils.util.import_class_at_runtime(args.workload_path)
    try:
        with psycopg.connect(dburl, autocommit=True) as conn:
            workload(args.args).init(conn)
    except Exception as e:
        logging.error(e)
        sys.exit(1)

    logging.info(
        "Init completed. Please update your database connection url to '%s'" % dburl)


def __init_create_schema(dburl: str, drop: bool, db_name: str, workload_path: str, dbms: str):
    """Create database and schema

    Args:
        dburl (str): the default connection string to the database
        drop (bool): whether to drop the current database
        db_name (str): the name of the database for the workload
        workload_path (str): filepath to the workload class file
        dbms (str): DBMS technology (CockroachDB, PostgreSQL, etc..)
    """
    # create the database according to the value passed in --init-db,
    # or use the workload name otherwise.
    # drop any existant database if --init-drop is True
    logging.debug("Running init_create_schema")
    try:
        with psycopg.connect(dburl, autocommit=True) as conn:
            with conn.cursor() as cur:
                if drop:
                    logging.debug("Dropping database '%s'" % db_name)
                    if dbms == "CockroachDB":
                        cur.execute(psycopg.sql.SQL("DROP DATABASE IF EXISTS {} CASCADE;").format(
                            psycopg.sql.Identifier(db_name)))
                    elif dbms == 'PostgreSQL':
                        cur.execute(psycopg.sql.SQL("DROP DATABASE IF EXISTS {};").format(
                            psycopg.sql.Identifier(db_name)))
                    else:
                        logging.error("DBMS not supported {dbms}")
                        sys.exit(1)

                # determine if database exists already
                # postgresql does not support CREATE DATABASE IF NOT EXISTS
                if cur.execute('SELECT 1 FROM pg_database WHERE datname = %s;', (db_name, )).fetchone() is None:
                    logging.debug("Creating database '%s'" % db_name)
                    cur.execute(psycopg.sql.SQL("CREATE DATABASE {};").format(
                        psycopg.sql.Identifier(db_name)))

                logging.info("Database '%s' created." % db_name)

    except Exception as e:
        logging.error("Exception: %s" % (e))
        sys.exit(1)

    dburl = pgworkload.utils.util.get_new_dburl(dburl, db_name)

    # now that we've created the database, connect to that database
    # and load the schema, which can be in a <workload>.sql file
    # or in the self.schema variable of the workload.

    # find if the .sql file exists
    schema_sql_file = os.path.abspath(
        pgworkload.utils.util.get_based_name_dir(workload_path) + '.sql')

    if os.path.exists(path=schema_sql_file):
        logging.debug('Found schema SQL file %s' % schema_sql_file)
        with open(schema_sql_file, 'r') as f:
            schema = f.read()
    else:
        logging.debug(
            f'Schema file {schema_sql_file} not found. Loading schema from the \'schema\' variable')
        try:
            workload = pgworkload.utils.util.import_class_at_runtime(
                path=workload_path)
            schema = workload({}).schema
        except AttributeError as e:
            logging.error(
                f'{e}. Make sure self.schema is a valid variable in __init__')
            sys.exit(1)
    try:
        with psycopg.connect(conninfo=dburl, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(query=psycopg.sql.SQL(schema))

            logging.info('Created workload schema')

    except Exception as e:
        logging.error(f'Exception: {e}')
        sys.exit(1)


def __init_generate_data(procs: int, workload_path: str, dbms: str, csv_max_rows: int):
    """Generate random datasets for the workload using SimpleFaker.
    CSV files will be saved in a directory named after the workload.

    Args:
        procs (int): count of concurrent processes to be used to generate the datasets
        workload_path (str): filepath to the workload class
        dbms (str): DBMS technology (CockroachDB, PostgreSQL, etc..)
    """
    logging.debug("Running init_generate_data")
    # description of how to generate the data is in workload variable self.load

    load = pgworkload.utils.util.get_workload_load(workload_path)
    if not load:
        logging.info(
            "Data generation definition file (.yaml) or variable (self.load) not defined. Skipping")
        return

    # get the dirname to put the csv files
    csv_dir: str = pgworkload.utils.util.get_based_name_dir(workload_path)

    # backup the current directory as to not override
    if os.path.isdir(csv_dir):
        os.rename(csv_dir, csv_dir + '.' +
                  dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S'))

    # create new directory
    os.mkdir(csv_dir)

    # for CockroachDB, we want gzip files for faster network transfer
    compression = 'gzip' if dbms == "CockroachDB" else None

    # generate the data by parsing the load variable
    pgworkload.utils.simplefaker.SimpleFaker(seed=0, csv_max_rows=csv_max_rows).generate(
        load, procs, csv_dir, '\t', compression)


def __init_import_data(dburl: str, workload_path: str, dbms:
                       str, http_server_hostname: str, http_server_port: str):
    """Import the datasets CSV files into the database

    Args:
        dburl (str): connection string to the database
        workload_path (str): filepath to the workload class
        dbms (str): DBMS technology (CockroachDB, PostgreSQL, etc..)
        http_server_hostname (str): The hostname of the server that serves the CSV files
        http_server_port (str): The port of the server that serves the CSV files
    """
    logging.debug("Running init_import_data")

    csv_dir = pgworkload.utils.util.get_based_name_dir(workload_path)
    load = pgworkload.utils.util.get_workload_load(workload_path)

    # Start the http server in a new Process
    mp.Process(target=pgworkload.utils.util.httpserver,
               args=(csv_dir, 3000), daemon=True).start()

    if os.path.isdir(csv_dir):
        csv_files = os.listdir(csv_dir)
    else:
        logging.debug("Nothing to import, skipping...")
        return

    try:
        with psycopg.connect(dburl, autocommit=True) as conn:
            with conn.cursor() as cur:
                node_count = 1
                if dbms == "CockroachDB":
                    # fetch the count of nodes that are part of the cluster
                    cur.execute(
                        "select count(*) from crdb_internal.gossip_nodes;")
                    node_count = cur.fetchone()[0]

                for table in load.keys():
                    logging.info("Importing data for table '%s'" % table)
                    table_csv_files = [
                        x for x in csv_files if x.split('.')[0] == table]

                    # chunked list is a list of list, where each item is a list of 'node_count' size.
                    chunked_list = [table_csv_files[i:i+node_count]
                                    for i in range(0, len(table_csv_files), node_count)]

                    # we import only 'node_count' items at a time, as
                    # we parallelize imports
                    for chunk in chunked_list:
                        if dbms == 'CockroachDB':
                            stmt = pgworkload.utils.util.get_import_stmt(chunk, table.replace(
                                '__', '.'), http_server_hostname, http_server_port)

                        elif dbms == 'PostgreSQL':
                            stmt = "COPY %s FROM '%s';" % (
                                table, os.path.join(os.getcwd(), chunk[0]))
                        else:
                            logging.warning(f'DBMS not supported: {dbms}')
                            pass

                        logging.debug(
                            f"'Importing files using command: '{stmt}'")

                        cur.execute(stmt)

    except Exception as e:
        logging.error(f'Exception: {e}')
        sys.exit(1)


