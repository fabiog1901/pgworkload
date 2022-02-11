#!/usr/bin/python

import argparse
import datetime as dt
import logging
import multiprocessing as mp
import os
import psycopg
import queue
import random
import re
import signal
import sys
import time
import urllib
import importlib
import socketserver
from pgworkload.simplefaker import SimpleFaker
from pgworkload import builtin_workloads
import yaml
import pgworkload.util
import traceback

DEFAULT_SLEEP = 5
SUPPORTED_DBMS = ["PostgreSQL", "CockroachDB"]


def main():
    try:
        args.func(args)
    except:
        args.parser.print_help()


def setup_parser():
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

    workload_parser.add_argument('-w', '--workload', dest='workload',
                                 help="Path to the workload module. Eg: workloads/bank.py for class 'Bank'")
    workload_parser.add_argument('--args', dest='args', default='{}',
                                 help='JSON string, or filepath to a JSON/YAML string, to pass to Workload')
    workload_parser.add_argument('--url', dest='dburl', default='postgres://root@localhost:26257/postgres?sslmode=disable',
                                 help="The connection string to the database. (default = 'postgres://root@localhost:26257/postgres?sslmode=disable')")
    workload_parser.add_argument('-a', '--app-name', dest='app_name',
                                 help='The application name specified by the client, if any. (default = <db name>)')
    workload_parser.add_argument('-c', "--concurrency", dest="concurrency",
                                 help="Number of concurrent workers (default = 1)", default=1, type=int)

    # root -> init
    root_init = root_sub.add_parser('init', help='Init commands',
                                    description='description: Run the workload',
                                    epilog='GitHub: <https://github.com/fabiog1901/pgworkload>',
                                    parents=[common_parser, workload_parser])
    root_init.add_argument('--drop', default=False, dest='drop', action=argparse.BooleanOptionalAction,
                           help="Drop the database if it exists")
    root_init.add_argument('--db', default='', dest='db', type=str,
                           help="Override the default DB name. (default = <value passed in --workload>)")
    root_init.add_argument('-d', '--delimiter', default='\t', dest='delimiter',
                           help="The delimeter char to use for the CSV files. (defaults = '\\t')")
    root_init.add_argument('--skip-schema', default=False, dest='skip_schema', action=argparse.BooleanOptionalAction,
                           help="Don't run the schema creation script")
    root_init.add_argument('--skip-gen', default=False, dest='skip_gen', action=argparse.BooleanOptionalAction,
                           help="Don't generate the CSV data files")
    root_init.add_argument('--skip-import', default=False, dest='skip_import', action=argparse.BooleanOptionalAction,
                           help="Don't import the CSV dataset files")
    root_init.set_defaults(parser=root_init)
    root_init.set_defaults(func=init)

    # root -> run
    root_run = root_sub.add_parser('run', help='Run commands',
                                   description='description: Run the workload',
                                   epilog='GitHub: <https://github.com/fabiog1901/pgworkload>',
                                   parents=[common_parser, workload_parser])
    root_run.add_argument('-k', '--conn-duration', dest='conn_duration', type=int, default=0,
                          help='The number of seconds to keep database connections alive before resetting them. (default = 0 --> ad infinitum)')
    root_run.add_argument('-s', '--stats-frequency', dest='frequency', type=int, default=10,
                          help='How often to display the stats in seconds. (default = 10)')
    root_run.add_argument('-i', '--iterations', dest="iterations", default=0, type=int,
                          help="Total number of iterations. (default = 0 --> ad infinitum)")
    root_run.add_argument('-d', '--duration', dest="duration", default=0, type=int,
                          help="Duration in seconds. (default = 0 --> ad infinitum)")
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
    root_util_csv.add_argument('-t', '--threads', dest="threads", default=1, type=int,
                               help="Number of concurrent threads/processes (default = 1)")
    root_util_csv.add_argument('-d', '--delimiter', default='\t', dest='delimiter',
                               help="The delimeter char to use for the CSV files. (default = '\\t')")
    root_util_csv.add_argument('-c', '--compression', default='gzip', dest='compression',
                               help="The compression format. (defaults = 'gzip')")
    root_util_csv.set_defaults(func=util_csv)

    return root.parse_args()


def util_yaml(args):
    with open(args.input, 'r') as f:
        ddl = f.read()

    if not args.output:
        output = get_based_name_dir(args.input) + '.yaml'
    else:
        output = args.output

    # backup the current file as to not override
    if os.path.exists(output):
        os.rename(output, output + '.' +
                  dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S'))

    # create new directory
    with open(output, 'w') as f:
        f.write(pgworkload.util.ddl_to_yaml(ddl))


def util_csv(args):
    with open(args.input, 'r') as f:
        load = yaml.safe_load(f.read())

    if not args.output:
        output_dir = get_based_name_dir(args.input)
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

    SimpleFaker(compression=args.compression).generate(
        load, args.threads, output_dir,  args.delimiter)


def run(args):
    global stats
    global concurrency
    concurrency = args.concurrency

    signal.signal(signal.SIGINT, signal_handler)

    workload, args = init_pgworkload(args)

    stats = pgworkload.util.Stats(frequency=args.frequency)

    if args.iterations > 0:
        args.iterations = int(args.iterations / args.concurrency)

    global kill_q
    global kill_q2

    q = mp.Queue(maxsize=1000)
    kill_q = mp.Queue()
    kill_q2 = mp.Queue()

    c = 0

    for _ in range(args.concurrency):
        mp.Process(target=worker, daemon=True, args=(
            q, kill_q, kill_q2, args.dburl, workload, args.args, args.iterations, args.duration, args.conn_duration)).start()

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

            if c >= args.concurrency:
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


def worker(q: mp.Queue, kill_q: mp.Queue, kill_q2: mp.Queue, dburl: str,
           workload: object, args: dict, iterations: int, duration: int, conn_duration: int):
    logging.debug("Worker created")
    # capture KeyboardInterrupt and do nothing
    signal.signal(signal.SIGINT, signal.SIG_IGN)

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
            conn_endtime = time.time() + conn_duration
        # listen for termination messages (poison pill)
        try:
            kill_q.get(block=False)
            logging.debug("Poison pill received")
            kill_q2.put(None)
            return
        except queue.Empty:
            pass
        try:
            with psycopg.connect(dburl, autocommit=True) as conn:
                logging.debug("Connection started")
                while True:

                    # listen for termination messages (poison pill)
                    try:
                        kill_q.get(block=False)
                        logging.debug("Poison pill received")
                        kill_q2.put(None)
                        return
                    except queue.Empty:
                        pass

                    # return if the limits of either iteration count and duration have been reached
                    if (iterations > 0 and c >= iterations) or \
                            (duration > 0 and time.time() >= endtime):
                        logging.debug("Task completed!")

                        # send task completed notification (a None)
                        q.put(None)
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
                        run_transaction(conn, lambda conn: txn(conn))
                        q.put((txn.__name__, time.time() - start))

                    c += 1
                    q.put(('__cycle__', time.time() - cycle_start))

        # catch any error, pass that error to the MainProcess
        except psycopg.errors.UndefinedTable as e:
            q.put(e)
            return
        except psycopg.Error as e:
            logging.error(f'{e.__class__.__name__} {e}')
            logging.info("Sleeping for %s seconds" % (DEFAULT_SLEEP))
            time.sleep(DEFAULT_SLEEP)
        except Exception as e:
            logging.error("Exception: %s" % (e))
            q.put(e)
            return


def init_pgworkload(args):
    logging.debug("Initialazing pgworkload")

    global concurrency
    concurrency = args.concurrency

    if not re.search(r'.*://.*/(.*)\?', args.dburl):
        logging.error(
            "The connection string needs to point to a database. Example: postgres://root@localhost:26257/postgres?sslmode=disable")
        sys.exit(1)

    workload = import_class_at_runtime(args.workload)

    args.dburl = set_query_parameter(
        args.dburl, "application_name", args.app_name if args.app_name else workload.__name__)

    logging.info("URL: '%s'" % args.dburl)

    # load args dict from file or string
    if os.path.exists(args.args):
        with open(args.args, 'r') as f:
            args.args = yaml.safe_load(f)
    else:
        args.args = yaml.safe_load(args.args)
        if isinstance(args.args, str):
            logging.error(
                "The value passed to '--args' is not a valid JSON or a valid path to a JSON/YAML file: '%s'" % args.args)
            sys.exit(1)

    return workload, args


def init(args):
    logging.debug("Running init")

    workload, args = init_pgworkload(args)

    if args.db == '':
        args.db = os.path.splitext(
            os.path.basename(args.workload))[0].lower()

    # PG or CRDB?
    try:
        dbms: str = get_dbms(args.dburl)
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
        init_create_schema(workload, args.dburl,
                           args.drop, args.db, args.workload, dbms)

    # PART 2 - GENERATE THE DATA
    if args.skip_gen:
        logging.debug("Skipping init_generate_data")
    else:
        init_generate_data(workload, args.concurrency,
                           args.workload, dbms, args.delimiter)

    # PART 3 - IMPORT THE DATA
    dburl = get_new_dburl(args.dburl, args.db)
    if args.skip_import:
        logging.debug("Skipping init_import_data")
    else:
        init_import_data(workload, dburl, args.workload, dbms)

    # PART 4 - RUN WORKLOAD INIT
    logging.debug("Running workload.init()")
    try:
        with psycopg.connect(dburl, autocommit=True) as conn:
            workload(args.args).init(conn)
    except Exception as e:
        logging.error(e)
        sys.exit(1)

    logging.info(
        "Init completed. Please update your database connection url to '%s'" % dburl)


def init_create_schema(workload: object, dburl: str, drop: bool, db_name: str, workload_path: str, dbms: str):
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
                    else:
                        cur.execute(psycopg.sql.SQL("DROP DATABASE IF EXISTS {};").format(
                            psycopg.sql.Identifier(db_name)))

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

    dburl = get_new_dburl(dburl, db_name)

    # now that we've created the database, connect to that database
    # and load the schema, which can be in a <workload>.sql file
    # or in the self.schema variable of the workload.

    # find if the .sql file exists
    schema_sql_file = os.path.abspath(os.path.join(os.path.dirname(workload_path), os.path.splitext(
        os.path.basename(workload_path))[0].lower() + '.sql'))

    if os.path.exists(schema_sql_file):
        logging.debug('Found schema SQL file %s' % schema_sql_file)
        with open(schema_sql_file, 'r') as f:
            schema = f.read()
    else:
        logging.debug(
            'Schema file %s not found. Loading schema from the \'schema\' variable', schema_sql_file)
        try:
            schema = workload.schema
        except AttributeError as e:
            logging.error(
                '%s. Make sure self.schema is a valid variable in __init__', e)
            sys.exit(1)
    try:
        with psycopg.connect(dburl, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(psycopg.sql.SQL(schema))

            logging.info('Created workload schema')

    except Exception as e:
        logging.error("Exception: %s" % (e))
        sys.exit(1)


def init_generate_data(workload: object, exec_threads: int, workload_path: str, dbms: str, delimiter: str):
    logging.debug("Running init_generate_data")
    # description of how to generate the data is in workload variable self.load

    load = get_workload_load(workload, workload_path)
    if not load:
        logging.info(
            "Data generation definition file (.yaml) or variable (self.load) not defined. Skipping")
        return

    # get the dirname to put the csv files
    csv_dir: str = get_based_name_dir(workload_path)

    # backup the current directory as to not override
    if os.path.isdir(csv_dir):
        os.rename(csv_dir, csv_dir + '.' +
                  dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S'))

    # create new directory
    os.mkdir(csv_dir)

    # for CockroachDB, we want gzip files for faster network transfer
    compression = 'gzip' if dbms == "CockroachDB" else None

    # generate the data by parsing the load variable
    SimpleFaker(compression=compression, seed=0).generate(
        load, exec_threads, csv_dir, delimiter)


def init_import_data(workload: object, dburl: str, workload_path: str, dbms: str):
    logging.debug("Running init_import_data")

    csv_dir = get_based_name_dir(workload_path)
    load = get_workload_load(workload, workload_path)

    # Start the http server in a new Process
    mp.Process(target=httpserver, args=(csv_dir, 3000), daemon=True).start()

    csv_files = os.listdir(csv_dir)

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
                    # this lists all files related to a table
                    # table_csv_files = []
                    # for x in csv_files:
                    #     l = x.split('.')
                    #     if '.'.join(l[:l.index('csv')-1]) == table:
                    #         table_csv_files.append(x)

                    table_csv_files = [
                        x for x in csv_files if x.split('.')[0] == table]

                    # chunked list is a list of list, where each item is a list of 'node_count' size.
                    chunked_list = [table_csv_files[i:i+node_count]
                                    for i in range(0, len(table_csv_files), node_count)]

                    # we import only 'node_count' items at a time, as
                    # we parallelize imports
                    for chunk in chunked_list:
                        if dbms == 'CockroachDB':
                            csv_data = ''
                            for x in chunk:
                                csv_data += "'http://localhost:3000/%s'," % x

                            stmt = (
                                "IMPORT INTO %s CSV DATA (%s) WITH delimiter = e'\t', nullif = '';" % (table.replace('__', '.'), csv_data[:-1]))

                        else:
                            stmt = "COPY %s FROM '%s';" % (
                                table, os.path.join(os.getcwd(), chunk[0]))

                        logging.debug('Importing files: %s' % str(chunk))
                        cur.execute(stmt)
    except Exception as e:
        logging.error("Exception: %s" % (e))
        sys.exit(1)


def signal_handler(sig, frame):
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


def httpserver(path, port=3000):
    """Create simple http server

    Args:
        path (string): The directory to serve files from
        port (int, optional): The http server listening port. Defaults to 3000.
    """
    os.chdir(path)

    try:
        with socketserver.TCPServer(server_address=("", port), RequestHandlerClass=pgworkload.util.QuietServerHandler) as httpd:
            httpd.serve_forever()
    except OSError as e:
        logging.error(e)
        return


def set_query_parameter(url, param_name, param_value):
    """convenience function to add a query parameter string such as '&application_name=myapp' to a url

    Args:
        url (str]): The URL string
        param_name (str): the parameter to add
        param_value (str): the value of the parameter

    Returns:
        str: the new URL with the added parameter
    """
    scheme, netloc, path, query_string, fragment = urllib.parse.urlsplit(url)
    query_params = urllib.parse.parse_qs(query_string)
    query_params[param_name] = [param_value]
    new_query_string = urllib.parse.urlencode(query_params, doseq=True)
    return urllib.parse.urlunsplit((scheme, netloc, path, new_query_string, fragment))


def import_class_at_runtime(path: str):
    """Imports a class with the same name of the module capitalized.
    Example: 'workloads/bank.py' returns class 'Bank' in module 'bank'

    Args:
        path (string): the path of the module to import

    Returns:
        class: the imported class
    """
    # check if path is one of the built-in workloads
    try:
        workload = getattr(builtin_workloads, path.lower().capitalize())
        logging.info("Loading built-in workload '%s'" %
                     path.lower().capitalize())
        return workload
    except AttributeError:
        pass

    # load the module at runtime
    sys.path.append(os.path.dirname(path))
    module_name = os.path.splitext(os.path.basename(path))[0]

    try:
        module = importlib.import_module(module_name)
        return getattr(module, module_name.capitalize())
    except AttributeError as e:
        logging.error(e)
        sys.exit(1)
    except ImportError as e:
        logging.error(e)
        sys.exit(1)


def run_transaction(conn, op, max_retries=3):
    """
    Execute the operation *op(conn)* retrying serialization failure.

    If the database returns an error asking to retry the transaction, retry it
    *max_retries* times before giving up (and propagate it).
    """
    for retry in range(1, max_retries + 1):
        try:
            op(conn)
            # If we reach this point, we were able to commit, so we break
            # from the retry loop.
            return
        except psycopg.errors.SerializationFailure as e:
            # This is a retry error, so we roll back the current
            # transaction and sleep for a bit before retrying. The
            # sleep time increases for each failed transaction.
            logging.debug("psycopg.SerializationFailure:: %s", e)
            conn.rollback()
            time.sleep((2 ** retry) * 0.1 * (random.random() + 0.5))
        except psycopg.Error as e:
            raise e

    raise ValueError(
        f"Transaction did not succeed after {max_retries} retries")


def get_based_name_dir(filepath):
    return os.path.join(os.path.dirname(filepath), os.path.splitext(
        os.path.basename(filepath))[0].lower())


def get_workload_load(workload: object, workload_path: str):
    # find if the .yaml file exists
    yaml_file = os.path.abspath(os.path.join(os.path.dirname(workload_path), os.path.splitext(
        os.path.basename(workload_path))[0].lower() + '.yaml'))

    if os.path.exists(yaml_file):
        logging.debug(
            'Found data generation definition YAML file %s' % yaml_file)
        with open(yaml_file, 'r') as f:
            return yaml.safe_load(f)
    else:
        logging.debug(
            'YAML file %s not found. Loading data generation definition from the \'load\' variable', yaml_file)
        try:
            return yaml.safe_load(workload.load)
        except AttributeError as e:
            logging.error(
                '%s. Make sure self.load is a valid variable in __init__', e)
            return {}


def get_new_dburl(dburl: str, db_name: str):
    """Return the dburl with the database name replaced.

    Args:
        dburl (str): the database connection string
        db_name (str): the new database name

    Returns:
        str: the new connection string
    """
    # craft the new dburl
    scheme, netloc, path, query_string, fragment = urllib.parse.urlsplit(dburl)
    path = '/' + db_name
    return urllib.parse.urlunsplit(
        (scheme, netloc, path, query_string, fragment))


def get_dbms(dburl: str):
    """Identify the DBMS technology

    Args:
        dburl: The connection string to the database

    Returns:
        str: the dmbs name (CockroachDB, PostgreSQL, ...)
    """
    try:
        with psycopg.connect(dburl, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("select version();")
                v: str = cur.fetchone()[0]
                x: str = v.split(" ")[0]
                if x not in SUPPORTED_DBMS:
                    raise ValueError("Unknown DBMS: %s" % x)
                return x
    except Exception as e:
        raise Exception(e)


args = setup_parser()

# setup global logging
logging.basicConfig(level=getattr(logging, vars(args).get('loglevel', 'INFO').upper(), logging.INFO),
                    format='%(asctime)s [%(levelname)s] (%(processName)s %(process)d) %(message)s')
