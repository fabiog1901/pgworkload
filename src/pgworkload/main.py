#!/usr/bin/python

import argparse
import datetime as dt
import json
import logging
import multiprocessing as mp
import os
import pgworkload.simplefaker
import pgworkload.util
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


def main():
    """This is the Entrypoint function
    """
    try:
        args.func(args)
    except AttributeError as e:
        logging.error(e)
        args.parser.print_help()
    except Exception as e:
        logging.error(e)


def setup_parser():
    """Parses CLI arguments

    Returns:
        (argparse.Namespace): The object with the passed arguments
    """
    # Common options to all parsers
    common_parser = argparse.ArgumentParser(add_help=False)

    common_parser.add_argument('-l', '--log-level', dest='loglevel', default='info',
                               help='The log level ([debug|info|warning|error]). (default = info)')

    # root
    root = argparse.ArgumentParser(description='pgworkload  - workload framework for the PostgreSQL protocol',
                                   epilog='GitHub: <https://github.com/fabiog1901/pgworkload>',
                                   parents=[])
    root.set_defaults(parser=root)

    root_sub = root.add_subparsers(help='')

    # workload options (common to root_init and root_run)
    workload_parser = argparse.ArgumentParser(add_help=False)

    workload_parser.add_argument('-w', '--workload', dest='workload_path',
                                 help="Path to the workload module. Eg: workloads/bank.py for class 'Bank'")
    workload_parser.add_argument('--args', dest='args', default='{}',
                                 help='JSON string, or filepath to a JSON/YAML string, to pass to Workload')
    workload_parser.add_argument('--url', dest='dburl', default='postgres://root@localhost:26257/postgres?sslmode=disable',
                                 help="The connection string to the database. (default = 'postgres://root@localhost:26257/postgres?sslmode=disable')")
    workload_parser.add_argument('-a', '--app-name', dest='app_name',
                                 help='The application name specified by the client, if any. (default = <db name>)')
    workload_parser.add_argument('-x', "--procs", dest="procs", type=int,
                                 help="Number of Processes to use (default = <system-cpu-count>)")

    # root -> init
    root_init = root_sub.add_parser('init', help='Init commands',
                                    description='description: Run the workload',
                                    epilog='GitHub: <https://github.com/fabiog1901/pgworkload>',
                                    parents=[common_parser, workload_parser])
    root_init.add_argument('--drop', default=False, dest='drop', action='store_true',
                           help="Drop the database if it exists")
    root_init.add_argument('--db', default='', dest='db', type=str,
                           help="Override the default DB name. (default = <value passed in --workload>)")
    root_init.add_argument('-n', '--hostname', dest='http_server_hostname', default='',
                           help="The hostname of the http server that serves the CSV files. (defaults = <inferred>)")
    root_init.add_argument('-p', '--port', dest='http_server_port', default='3000',
                           help="The port of the http server that servers the CSV files. (defaults = 3000)")
    root_init.add_argument('-s', '--skip-schema', default=False, dest='skip_schema', action='store_true',
                           help="Don't run the schema creation script")
    root_init.add_argument('-g', '--skip-gen', default=False, dest='skip_gen', action='store_true',
                           help="Don't generate the CSV data files")
    root_init.add_argument('-i', '--skip-import', default=False, dest='skip_import', action='store_true',
                           help="Don't import the CSV dataset files")
    root_init.add_argument('--csv-max-rows', default=100000, dest='csv_max_rows', type=int,
                           help="Max count of rows per resulting CSV file")

    root_init.set_defaults(parser=root_init)
    root_init.set_defaults(func=init)

    # root -> run
    root_run = root_sub.add_parser('run', help='Run commands',
                                   description='description: Run the workload',
                                   epilog='GitHub: <https://github.com/fabiog1901/pgworkload>',
                                   parents=[common_parser, workload_parser])
    root_run.add_argument('-c', "--concurrency", dest="concurrency", default='1', type=int,
                          help="Number of concurrent workers (default = 1)")
    root_run.add_argument('-k', '--conn-duration', dest='conn_duration', type=int, default=0,
                          help='The number of seconds to keep database connections alive before resetting them. (default = 0 --> ad infinitum)')
    root_run.add_argument('-s', '--stats-frequency', dest='frequency', type=int, default=10,
                          help='How often to display the stats in seconds. (default = 10)')
    root_run.add_argument('-i', '--iterations', dest="iterations", default=0, type=int,
                          help="Total number of iterations. (default = 0 --> ad infinitum)")
    root_run.add_argument('-d', '--duration', dest="duration", default=0, type=int,
                          help="Duration in seconds. (default = 0 --> ad infinitum)")
    root_run.add_argument('-r', '--ramp', dest="ramp", default=0, type=int,
                          help="Ramp up time in seconds. (defaut = 0)")
    root_run.add_argument('-p', '--port', dest='prom_port', default='26260', type=int,
                          help="The port of the Prometheus server. (default = 26260)")
    root_run.add_argument('--no-autocommit', default=True, dest='autocommit', action='store_false',
                          help="Configure the psycopg Connection with autocommit. (default = True)")
    root_run.set_defaults(parser=root_run)
    root_run.set_defaults(func=run)

    # root -> util
    root_util = root_sub.add_parser('util', help='Utility commands',
                                    description='description: Generate YAML data generation files and CSV datasets',
                                    epilog='GitHub: <https://github.com/fabiog1901/pgworkload>',)

    root_util.set_defaults(parser=root_util)
    root_util_sub = root_util.add_subparsers()

    # root -> util -> yaml
    root_util_yaml = root_util_sub.add_parser('yaml', help='Generate YAML data generation file from a DDL SQL file',
                                              description='description: Generate YAML data generation file from a DDL SQL file',
                                              epilog='GitHub: <https://github.com/fabiog1901/pgworkload>',
                                              parents=[common_parser])
    root_util_yaml.add_argument('-i', '--input', dest='input', type=str, default='',
                                help='Filepath to the DDL SQL file')
    root_util_yaml.add_argument('-o', '--output', dest='output', type=str, default='',
                                help='Output filepath. (default = <input-basename>.yaml)')
    root_util_yaml.set_defaults(func=util_yaml)

    # root -> util -> csv
    root_util_csv = root_util_sub.add_parser('csv', help='Generate CSV files from a a YAML data generation file',
                                             description='description: Generate CSV files from a a YAML data generation file',
                                             epilog='GitHub: <https://github.com/fabiog1901/pgworkload>',
                                             parents=[common_parser])
    root_util_csv.add_argument('-i', '--input', dest='input', type=str,
                               help='Filepath to the YAML data generation file')
    root_util_csv.add_argument('-o', '--output', dest='output', type=str, default='',
                               help='Output directory for the CSV files. (default = <input-basename>)')
    root_util_csv.add_argument('-x', '--procs', dest="procs", type=int,
                               help="Number of concurrent processes (default = <system-cpu-count>)")
    root_util_csv.add_argument('-d', '--delimiter', default='\t', dest='delimiter',
                               help="The delimeter char to use for the CSV files. (default = '\\t')")
    root_util_csv.add_argument('-c', '--compression', default='', dest='compression',
                               help="The compression format. (defaults = '' (No compression))")
    root_util_csv.add_argument('--csv-max-rows', default=100000, dest='csv_max_rows', type=int,
                               help="Max count of rows per resulting CSV file")
    root_util_csv.add_argument('-t', '--table-name', dest='table_name', default='table_name',
                               help="The table name used in the import statement. (defaults = table_name)")
    root_util_csv.add_argument('-n', '--hostname', dest='http_server_hostname', default='',
                               help="The hostname of the http server that serves the CSV files. (defaults = <inferred>)")
    root_util_csv.add_argument('-p', '--port', dest='http_server_port', default='3000',
                               help="The port of the http server that servers the CSV files. (defaults = 3000)")
    root_util_csv.set_defaults(func=util_csv)

    return root.parse_args()


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

    workload = pgworkload.util.import_class_at_runtime(path=args.workload_path)

    args.dburl = pgworkload.util.set_query_parameter(url=args.dburl, param_name="application_name",
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


def run(args: argparse.Namespace):
    """Run the workload

    Args:
        args (argparse.Namespace): the args passed at the CLI
    """

    global stats
    args = init_pgworkload(args)

    global concurrency

    concurrency = int(args.concurrency)


    workload = pgworkload.util.import_class_at_runtime(path=args.workload_path)

    signal.signal(signal.SIGINT, signal_handler)

    stats = pgworkload.util.Stats(
        frequency=args.frequency, prom_port=args.prom_port)

    if args.iterations > 0:
        args.iterations = int(args.iterations / concurrency)

    global kill_q
    global kill_q2

    q = mp.Queue(maxsize=1000)
    kill_q = mp.Queue()
    kill_q2 = mp.Queue()

    c = 0

    threads_per_proc = pgworkload.util.get_threads_per_proc(
        args.procs, args.concurrency)

    ramp_intervals = int(args.ramp / len(threads_per_proc))

    for x in threads_per_proc:
        mp.Process(target=worker, daemon=True, args=(
            x-1, q, kill_q, kill_q2, args.dburl, args.autocommit, workload, args.args, args.iterations, args.duration, args.conn_duration)).start()
        time.sleep(ramp_intervals)
        
    try:
        stat_time = time.time() + args.frequency
        while True:
            try:
                # read from the queue for stats or completion messages
                tup = q.get(block=False)
                if isinstance(tup, tuple):
                    stats.add_latency_measurement(*tup)
                else:
                    c += 1
            except queue.Empty:
                pass

            if c >= concurrency:
                if isinstance(tup, psycopg.errors.UndefinedTable):
                    logging.error(tup)
                    logging.error(
                        "The schema is not present. Did you initialize the workload?")
                    sys.exit(1)
                elif isinstance(tup, Exception):
                    logging.error("Exception raised: %s" % tup)
                    sys.exit(1)
                else:
                    logging.info(
                        "Requested iteration/duration limit reached. Printing final stats")
                    stats.print_stats()
                    sys.exit(0)

            if time.time() >= stat_time:
                stats.print_stats()
                stats.new_window()
                stat_time = time.time() + args.frequency

    except Exception as e:
        logging.error(e)


def worker(thread_count: int, q: mp.Queue, kill_q: mp.Queue, kill_q2: mp.Queue,
           dburl: str, autocommit: bool,
           workload: object, args: dict, iterations: int, duration: int, conn_duration: int,
           threads: list = []):
    """Process worker function to run the workload in a multiprocessing env

    Args:
        thread_count(int): The number of threads to create
        q (mp.Queue): queue to report query metrics
        kill_q (mp.Queue): queue to handle stopping the worker
        kill_q2 (mp.Queue): queue to handle stopping the worker
        dburl (str): connection string to the database
        autocommit (bool): whether to set autocommit for the connection
        workload (object): workload class object
        args (dict): args to init the workload class
        iterations (int): count of workload iteration before returning
        duration (int): seconds before returning
        conn_duration (int): seconds before restarting the database connection
        threads (list): the list of threads to wait to finish before returning
    """
    threads: list[threading.Thread] = []

    for _ in range(thread_count):
        thread = threading.Thread(
            target=worker,
            daemon=True, args=(0,
                               q, kill_q, kill_q2, dburl, autocommit,
                               workload, args, iterations,
                               duration, conn_duration, [])
        )
        thread.start()
        threads.append(thread)

    if threading.current_thread().name == 'MainThread':
        logging.debug("Process Worker created")
        # capture KeyboardInterrupt and do nothing
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    else:
        logging.debug("Thread Worker created")

    # catch exception while instantiating the workload class
    try:
        w = workload(args)
    except Exception as e:
        stack_lines = traceback.format_exc()
        q.put(Exception(stack_lines))
        return

    c = 0
    endtime = 0
    conn_endtime = 0

    if duration > 0:
        endtime = time.time() + duration

    while True:
        if conn_duration > 0:
            # reconnect every conn_duration +/-10%
            conn_endtime = time.time() + int(conn_duration * random.uniform(.9, 1.1))
        # listen for termination messages (poison pill)
        try:
            kill_q.get(block=False)
            logging.debug("Poison pill received")
            kill_q2.put(None)
            for x in threads:
                x.join()

            return
        except queue.Empty:
            pass
        try:
            with psycopg.connect(dburl, autocommit=autocommit) as conn:
                logging.debug("Connection started")
                while True:

                    # listen for termination messages (poison pill)
                    try:
                        kill_q.get(block=False)
                        logging.debug("Poison pill received")
                        kill_q2.put(None)
                        for x in threads:
                            x.join()
                        return
                    except queue.Empty:
                        pass

                    # return if the limits of either iteration count and duration have been reached
                    if (iterations > 0 and c >= iterations) or \
                            (duration > 0 and time.time() >= endtime):
                        logging.debug("Task completed!")

                        # send task completed notification (a None)
                        q.put(None)
                        for x in threads:
                            x.join()
                        return

                    # break from the inner loop if limit for connection duration has been reached
                    # this will cause for the outer loop to reset the timer and restart with a new conn
                    if conn_duration > 0 and time.time() >= conn_endtime:
                        logging.debug(
                            "conn_duration reached, will reset the connection.")
                        break

                    cycle_start = time.time()
                    for txn in w.run():
                        start = time.time()
                        pgworkload.util.run_transaction(
                            conn, lambda conn: txn(conn))
                        q.put((txn.__name__, time.time() - start))

                    c += 1
                    q.put(('__cycle__', time.time() - cycle_start))

        # catch any error, pass that error to the MainProcess
        except psycopg.errors.UndefinedTable as e:
            q.put(e)
            return
        # psycopg.OperationalErrors can either mean a disconnection
        # or some other errors.
        # We don't stop if a node goes doesn, instead, wait few seconds and attempt
        # a new connection.
        # If the error is not beacuse of a disconnection, then unfortunately
        # the worker will continue forever
        except psycopg.Error as e:
            logging.error(f'{e.__class__.__name__} {e}')
            logging.info("Sleeping for %s seconds" % (DEFAULT_SLEEP))
            time.sleep(DEFAULT_SLEEP)
        except Exception as e:
            logging.error("Exception: %s" % (e))
            q.put(e)
            return


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
        dbms: str = pgworkload.util.get_dbms(args.dburl)
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
    dburl = pgworkload.util.get_new_dburl(args.dburl, args.db)
    if args.skip_import:
        logging.debug("Skipping init_import_data")
    else:
        if not args.http_server_hostname:
            args.http_server_hostname = pgworkload.util.get_hostname()
            logging.debug(
                f"Hostname identified as: '{args.http_server_hostname}'")

        __init_import_data(dburl, args.workload_path, dbms,
                           args.http_server_hostname, args.http_server_port)

    # PART 4 - RUN WORKLOAD INIT
    logging.debug("Running workload.init()")
    workload = pgworkload.util.import_class_at_runtime(args.workload_path)
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

    dburl = pgworkload.util.get_new_dburl(dburl, db_name)

    # now that we've created the database, connect to that database
    # and load the schema, which can be in a <workload>.sql file
    # or in the self.schema variable of the workload.

    # find if the .sql file exists
    schema_sql_file = os.path.abspath(
        pgworkload.util.get_based_name_dir(workload_path) + '.sql')

    if os.path.exists(path=schema_sql_file):
        logging.debug('Found schema SQL file %s' % schema_sql_file)
        with open(schema_sql_file, 'r') as f:
            schema = f.read()
    else:
        logging.debug(
            f'Schema file {schema_sql_file} not found. Loading schema from the \'schema\' variable')
        try:
            workload = pgworkload.util.import_class_at_runtime(
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

    load = pgworkload.util.get_workload_load(workload_path)
    if not load:
        logging.info(
            "Data generation definition file (.yaml) or variable (self.load) not defined. Skipping")
        return

    # get the dirname to put the csv files
    csv_dir: str = pgworkload.util.get_based_name_dir(workload_path)

    # backup the current directory as to not override
    if os.path.isdir(csv_dir):
        os.rename(csv_dir, csv_dir + '.' +
                  dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S'))

    # create new directory
    os.mkdir(csv_dir)

    # for CockroachDB, we want gzip files for faster network transfer
    compression = 'gzip' if dbms == "CockroachDB" else None

    # generate the data by parsing the load variable
    pgworkload.simplefaker.SimpleFaker(seed=0, csv_max_rows=csv_max_rows).generate(
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

    csv_dir = pgworkload.util.get_based_name_dir(workload_path)
    load = pgworkload.util.get_workload_load(workload_path)

    # Start the http server in a new Process
    mp.Process(target=pgworkload.util.httpserver,
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
                            stmt = pgworkload.util.get_import_stmt(chunk, table.replace(
                                '__', '.'), http_server_hostname, http_server_port)

                        elif dbms == 'PostgreSQL':
                            stmt = "COPY %s FROM '%s';" % (
                                table, os.path.join(os.getcwd(), chunk[0]))
                        else:
                            logging.warning(f'DBMS not supported: {dbms}')
                            pass

                        logging.debug(f"'Importing files using command: '{stmt}'")
                        
                        cur.execute(stmt)

    except Exception as e:
        logging.error(f'Exception: {e}')
        sys.exit(1)


def util_csv(args: argparse.Namespace):
    """Wrapper around SimpleFaker to create CSV datasets
    given an input YAML data gen definition file

    Args:
        args (argparse.Namespace): args passed at the CLI
    """
    if not args.input:
        logging.error("No input argument was passed")
        print()
        args.parser.print_help()
        sys.exit(1)

    with open(args.input, 'r') as f:
        load = yaml.safe_load(f.read())

    if not args.output:
        output_dir = pgworkload.util.get_based_name_dir(args.input)
    else:
        output_dir = args.output

    # backup the current directory as to not override
    if os.path.isdir(output_dir):
        os.rename(output_dir, output_dir + '.' +
                  dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S'))

    # if the output dir is
    if os.path.exists(output_dir):
        output_dir += '_dir'

    # create new directory
    os.mkdir(output_dir)

    if not args.compression:
        args.compression = None

    if not args.procs:
        args.procs = os.cpu_count()

    pgworkload.simplefaker.SimpleFaker(csv_max_rows=args.csv_max_rows).generate(
        load, int(args.procs), output_dir,  args.delimiter, args.compression)

    csv_files = os.listdir(output_dir)

    if not args.http_server_hostname:
        args.http_server_hostname = pgworkload.util.get_hostname()
        logging.debug(
            f"Hostname identified as: '{args.http_server_hostname}'")

    stmt = pgworkload.util.get_import_stmt(
        csv_files, args.table_name, args.http_server_hostname, args.http_server_port)

    print(stmt)


def util_yaml(args: argparse.Namespace):
    """Wrapper around util function ddl_to_yaml() for 
    crafting a data gen definition YAML string from 
    CREATE TABLE statements.

    Args:
        args (argparse.Namespace): args passed at the CLI
    """
    if not args.input:
        logging.error("No input argument was passed")
        print()
        args.parser.print_help()
        sys.exit(1)

    with open(args.input, 'r') as f:
        ddl = f.read()

    if not args.output:
        output = pgworkload.util.get_based_name_dir(args.input) + '.yaml'
    else:
        output = args.output

    # backup the current file as to not override
    if os.path.exists(output):
        os.rename(output, output + '.' +
                  dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S'))

    # create new directory
    with open(output, 'w') as f:
        f.write(pgworkload.util.ddl_to_yaml(ddl))


args: argparse.Namespace = setup_parser()

# setup global logging
logging.basicConfig(level=getattr(logging, vars(args).get('loglevel', 'INFO').upper(), logging.INFO),
                    format='%(asctime)s [%(levelname)s] (%(processName)s %(process)d %(threadName)s) %(message)s')
