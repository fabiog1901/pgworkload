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


def setup_parser():
    parser = argparse.ArgumentParser()
    
    subparsers = parser.add_subparsers(help='sub-command help')
    
    # Util
    subparser_gen = subparsers.add_parser('gen', help='generate auxiliary files')
    subparser_gen.add_argument('--from-ddl', dest='from_ddl', type=str, default='',
                               help='Read DDL SQL file and generates YAML data generation file')
    subparser_gen.add_argument('--from-yaml', dest='from_yaml', type=str, default='',
                               help='Read YAML data generation file and generate CSV files')
    subparser_gen.add_argument('--output-yaml', dest='output_yaml', type=str, default='',
                               help='Filepath for the generated YAML file')
    subparser_gen.add_argument('--output-csv-dir', dest='output_csv_dir', type=str, default='',
                               help='Output directory for the CSV files')
    
    # main
    parser.add_argument('--url', dest='dburl', default='postgres://root@localhost:26257/defaultdb?sslmode=disable',
                        help="The connection string to the database. Default is 'postgres://root@localhost:26257/defaultdb?sslmode=disable'")
    parser.add_argument('--app-name', dest='app_name',
                        help='The name that can be used for filtering statements by client in the DB Console')
    parser.add_argument("--concurrency", dest="concurrency",
                        help="Number of concurrent workers (default 1)", default=1, type=int)
    parser.add_argument("--iterations", dest="iterations", default=0, type=int,
                        help="Total number of iterations (default=0 --> ad infinitum)")
    parser.add_argument("--duration", dest="duration", default=0, type=int,
                        help="Duration in seconds (default=0 --> ad infinitum)")

    # initialization args
    parser.add_argument('--init', default=False, dest='init', action=argparse.BooleanOptionalAction,
                        help="Run a one-off initialization step")
    parser.add_argument('--init-drop', default=False, dest='init_drop', action=argparse.BooleanOptionalAction,
                        help="On initialization, drop the database if it exists")
    parser.add_argument('--init-db', default='', dest='init_db', type=str,
                        help="On initialization, override the default db name. Defaults to value passed in --workload-class or, if absent, --workload")
    parser.add_argument('--init-delimiter', default='\t', dest='delimiter',
                        help="On initialization, the delimeter char to use for the CSV files. Defaults = '\t'")
    parser.add_argument('--init-skip-create-schema', default=False, dest='init_skip_create_schema', action=argparse.BooleanOptionalAction,
                        help="On initialization, don't run the schema creation script")
    parser.add_argument('--init-skip-data-generation', default=False, dest='init_skip_data_generation', action=argparse.BooleanOptionalAction,
                        help="On initialization, don't generate the CSV data files")
    parser.add_argument('--init-skip-data-import', default=False, dest='init_skip_data_import', action=argparse.BooleanOptionalAction,
                        help="On initialization, don't import")
    # other params
    parser.add_argument('--log-level', dest='loglevel', default='info',
                        help='The log level ([debug|info|warning|error]). (default = info)')
    parser.add_argument('--conn-duration', dest='conn_duration', type=int, default=0,
                        help='The number of seconds to keep database connections alive before resetting them (default=0 --> ad infinitum)')
    parser.add_argument('--stats-frequency', dest='frequency', type=int, default=10,
                        help='How often to display the stats in seconds (default=10). Set 0 to suppress stats printing')
    parser.add_argument('--workload', dest='workload', #required=True,
                        help="Path to the workload module. Eg: workloads/bank.py for class 'Bank'")
    parser.add_argument('--args', dest='args', default='{}',
                        help='JSON string, or filepath to a JSON/YAML string, to pass to Workload at runtime')

    return parser.parse_args()


def main():
    global stats

    signal.signal(signal.SIGINT, signal_handler)
   
    try:
        if args.from_ddl:
            pgworkload.util.ddl_to_yaml(args.from_ddl, args.output_yaml)
            sys.exit(0)
        
        if args.from_yaml:
            with open(args.from_yaml, 'r') as f:
                load = yaml.safe_load(f.read())

            SimpleFaker().generate(load, args.concurrency, args.output_csv_dir,  args.delimiter)
            sys.exit(0)
    except:
        pass
        
    workload = import_class_at_runtime(args.workload)
    
    stats = pgworkload.util.Stats(frequency=args.frequency)

    if not re.search(r'.*://.*/(.*)\?', args.dburl):
        logging.error(
            "The connection string needs to point to a database. Example: postgres://root@localhost:26257/postgres?sslmode=disable")
        sys.exit(1)

    if args.iterations > 0:
        args.iterations = int(args.iterations / args.concurrency)

    if args.init_db == '':
        args.init_db = os.path.splitext(
            os.path.basename(args.workload))[0].lower()

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

    args.dburl = set_query_parameter(
        args.dburl, "application_name", args.app_name if args.app_name else workload.__name__)
    logging.info("URL: '%s'" % args.dburl)

    if args.init:
        init(workload(args.args))
        sys.exit(0)

    q = mp.Queue(maxsize=1000)
    global kill_q
    kill_q = mp.Queue()
    global kill_q2
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
                elif isinstance(tup, psycopg.errors.OperationalError):
                    logging.error(tup)
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


def signal_handler(sig, frame):
    global stats

    logging.info("KeyboardInterrupt signal detected. Stopping processes...")

    # send the poison pill to each worker
    for _ in range(args.concurrency):
        kill_q.put(None)
    
    # wait until all workers return
    start = time.time()
    
    c = 0
    timeout = True

    while c < args.concurrency and timeout: 
        try:
            kill_q2.get(block=False)
            c += 1
        except:
            pass

        time.sleep(0.1)
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


def get_csv_files_dirname():
    return os.path.join(os.path.dirname(args.workload), os.path.splitext(
        os.path.basename(args.workload))[0].lower())


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


def init(workload: object):
    logging.debug("Running init")
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
    if args.init_skip_create_schema:
        logging.debug("Skipping init_create_schema")
    else:
        init_create_schema(workload, args.dburl,
                           args.init_drop, args.init_db, args.workload, dbms)

    # PART 2 - GENERATE THE DATA
    if args.init_skip_data_generation:
        logging.debug("Skipping init_generate_data")
    else:
        init_generate_data(workload, args.concurrency, args.workload, dbms)

    # PART 3 - IMPORT THE DATA
    dburl = get_new_dburl(args.dburl, args.init_db)
    if args.init_skip_data_import:
        logging.debug("Skipping init_import_data")
    else:
        init_import_data(workload, dburl, args.workload, dbms)

    # PART 4 - RUN WORKLOAD INIT
    logging.debug("Running workload.init()")
    try:
        with psycopg.connect(dburl, autocommit=True) as conn:
            workload.init(conn)
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


def init_generate_data(workload: object, exec_threads: int, workload_path: str, dbms: str):
    logging.debug("Running init_generate_data")
    # description of how to generate the data is in workload variable self.load

    load = get_workload_load(workload, workload_path)
    if not load:
        logging.info(
            "Data generation definition file (.yaml) or variable (self.load) not defined. Skipping")
        return

    # get the dirname to put the csv files
    csv_dir: str = get_csv_files_dirname()

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
        load, exec_threads, csv_dir, args.delimiter)


def init_import_data(workload: object, dburl: str, workload_path: str, dbms: str):
    logging.debug("Running init_import_data")

    csv_dir = get_csv_files_dirname()
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
        except psycopg.errors.OperationalError as e:
            q.put(e)
            return
        except psycopg.Error as e:
            logging.error(
                "Lost connection to the database. Sleeping for %s seconds." % (DEFAULT_SLEEP))
            logging.error(e)
            time.sleep(DEFAULT_SLEEP)
        except Exception as e:
            logging.error("Exception: %s" % (e))


args = setup_parser()

# setup global logging
logging.basicConfig(level=getattr(logging, args.loglevel.upper(), logging.INFO),
                    format='%(asctime)s [%(levelname)s] (%(processName)s %(process)d) %(message)s')
