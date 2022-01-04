#!/usr/bin/python

import argparse
import logging
import multiprocessing as mp
import numpy
import os
import psycopg
import queue
import random
import re
import signal
import sys
import tabulate
import time
import urllib
import importlib

DEFAULT_SLEEP = 5


class Stats:
    def __init__(self, frequency):
        self.cumulative_counts = {}
        self.instantiation_time = time.time()
        # self.mutex = threading.Lock()
        self.frequency = frequency
        self.new_window()

    # reset stats while keeping cumulative counts
    def new_window(self):
        # self.mutex.acquire()
        try:
            self.window_start_time = time.time()
            self.window_stats = {}
        finally:
            pass
            # self.mutex.release()

    # add one latency measurement in seconds
    def add_latency_measurement(self, action, measurement):
        # self.mutex.acquire()
        try:
            self.window_stats.setdefault(action, []).append(measurement)
            self.cumulative_counts.setdefault(action, 0)
            self.cumulative_counts[action] += 1
        finally:
            pass
            # self.mutex.release()

    # print the current stats this instance has collected.
    # If action_list is empty, it will only prevent rows it has captured this period, otherwise it will print a row for each action.
    def print_stats(self, action_list=[]):
        def get_percentile_measurement(action, percentile):
            return numpy.percentile(self.window_stats.setdefault(action, [0]), percentile)

        def get_stats_row(action):
            elapsed = time.time() - self.instantiation_time

            if action in self.window_stats:
                return [action,
                        round(elapsed, 0),
                        self.cumulative_counts[action],
                        round(self.cumulative_counts[action] / elapsed, 2),
                        len(self.window_stats[action]),
                        round(
                            len(self.window_stats[action]) / self.frequency, 2),
                        round(float(get_percentile_measurement(
                            action, 50)) * 1000, 2),
                        round(float(get_percentile_measurement(
                            action, 90)) * 1000, 2),
                        round(float(get_percentile_measurement(
                            action, 95)) * 1000, 2),
                        round(float(get_percentile_measurement(
                            action, 99)) * 1000, 2)]
            else:
                return [action, round(elapsed, 0), self.cumulative_counts.get(action, 0), 0, 0, 0, 0, 0, 0]

        header = ["transaction name", "elapsed_time",  "total_ops", "tot_ops/second",
                  "period_ops", "period_ops/second", "p50(ms)", "p90(ms)", "p95(ms)", "p99(ms)"]
        rows = []

        # self.mutex.acquire()
        try:
            if len(action_list):
                for action in sorted(action_list):
                    rows.append(get_stats_row(action))
            else:
                for action in sorted(list(self.window_stats)):
                    rows.append(get_stats_row(action))
            print(tabulate.tabulate(rows, header), "\n")
        finally:
            pass
            # self.mutex.release()


def main():
    global stats

    signal.signal(signal.SIGINT, signal_handler)

    workload = import_class_at_runtime(args.workload, args.workload_class)

    stats = Stats(frequency=args.frequency)

    if not re.search(r'.*://.*/(.*)\?', args.dburl):
        logging.error(
            "The connection string needs to point to a database. Example: postgres://root@localhost:26257/defaultdb?sslmode=disable")
        sys.exit(1)

    if args.iterations > 0:
        args.iterations = int(args.iterations / args.concurrency)

    args.dburl = set_query_parameter(
        args.dburl, "application_name", args.app_name if args.app_name else workload.__name__)
    logging.info("dburl: '%s'" % args.dburl)

    if args.init:
        init()
        sys.exit(0)

    q = mp.Queue(maxsize=1000)
    global kill_q
    kill_q = mp.JoinableQueue()

    c = 0

    for _ in range(args.concurrency):
        mp.Process(target=worker, args=(
            q, kill_q, args.dburl, workload, args.parameters, args.iterations, args.duration, args.conn_duration)).start()

    try:
        stat_time = time.time() + args.frequency
        while True:
            try:
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
    kill_q.join()

    logging.info("Printing final stats")
    stats.print_stats()
    sys.exit(0)


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


def import_class_at_runtime(module: str, class_name: str):
    """Imports a class with the same name of the module unless class_name is passed.

    Args:
        module (string): the path of the module to import, eg: workloads/bank.py
        class_name (string): the name of the class to import, eg: bank2

    Returns:
        class: the imported class
    """
    if module.endswith('.py'):
        module = module[:-3]
    module = module.replace('/', '.')
    class_name = module.split('.')[-1] if class_name is None else class_name
    try:
        pkg = importlib.import_module(module)
        return getattr(pkg, class_name)
    except AttributeError:
        logging.error("could not import %s" % class_name)
        sys.exit(1)
    except ImportError:
        logging.error("could not import %s" % class_name)
        sys.exit(1)


def setup_parser():
    parser = argparse.ArgumentParser()
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

    parser.add_argument('--log-level', dest='loglevel', default='info',
                        help='The log level ([debug|info|warning|error]). (default = info)')
    parser.add_argument('--conn-duration', dest='conn_duration', type=int, default=0,
                        help='The number of seconds to keep database connections alive before resetting them (default=0 --> ad infinitum)')
    parser.add_argument('--stats-frequency', dest='frequency', type=int, default=10,
                        help='How often to display the stats in seconds (default=10)')
    parser.add_argument('--workload', dest='workload', required=True,
                        help="Path to the workload module. Eg: workloads/bank.py")
    parser.add_argument('--workload-class', dest='workload_class', type=str,
                        help="The workload class module, if different from the module basename")
    parser.add_argument('--parameters', dest='parameters', nargs='*', default=[],
                        help='parameters to pass to Workload at runtime')
    return parser.parse_args()


def run_transaction(conn, op, max_retries=3):
    for retry in range(1, max_retries + 1):
        try:
            op(conn)
            return
        except psycopg.errors.SerializationFailure as e:
            logging.debug("psycopg.SerializationFailure:: %s", e)
            conn.rollback()
            time.sleep((2 ** retry) * 0.1 * (random.random() + 0.5))
        except psycopg.Error as e:
            raise e

    raise ValueError(
        f"Transaction did not succeed after {max_retries} retries")


def init():
    logging.debug("Running init script")

    try:
        with psycopg.connect(args.dburl, autocommit=True) as conn:

            if args.init_db == '':
                args.init_db = os.path.splitext(
                    os.path.basename(args.workload))[0].lower()

            with conn.cursor() as cur:
                if args.init_drop:
                    cur.execute(psycopg.sql.SQL("DROP DATABASE IF EXISTS {} CASCADE;").format(
                        psycopg.sql.Identifier(args.init_db)))
                cur.execute(psycopg.sql.SQL("CREATE DATABASE IF NOT EXISTS {};").format(
                    psycopg.sql.Identifier(args.init_db)))

        # craft the new dburl
        scheme, netloc, path, query_string, fragment = urllib.parse.urlsplit(
            args.dburl)
        path = '/' + args.init_db
        args.dburl = urllib.parse.urlunsplit(
            (scheme, netloc, path, query_string, fragment))

        # now that we've created the database, connect to that database
        with psycopg.connect(args.dburl, autocommit=True) as conn:
            # find if the .sql file exists
            ddl_sql_file = os.path.join(os.path.dirname(args.workload), os.path.splitext(
                os.path.basename(args.workload))[0].lower() + '.sql')

            if os.path.exists(ddl_sql_file):
                logging.debug('Found SQL file %s' % ddl_sql_file)
                with open(ddl_sql_file, 'r') as f:
                    with conn.cursor() as cur:
                        cur.execute(psycopg.sql.SQL(f.read()))

        # run_transaction(conn, lambda conn: txn(conn))

        logging.info(
            "Init completed. Please update your database connection url to '%s'" % args.dburl)

    except Exception as e:
        logging.error("Exception: %s" % (e))


def worker(q: mp.Queue, kill_q: mp.JoinableQueue, dburl: str, workload: object, parameters: list, iterations: int, duration: int, conn_duration: int):
    logging.debug("worker created")

    # capture KeyboardInterrupt and do nothing
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    w = workload(parameters)
    c = 0
    endtime = 0
    conn_endtime = 0

    if duration > 0:
        endtime = time.time() + duration

    while True:
        if conn_duration > 0:
            conn_endtime = time.time() + conn_duration

        try:
            with psycopg.connect(dburl, autocommit=True) as conn:
                logging.debug("connection started")
                while True:
                    try:
                        # listen for termination messages
                        kill_q.get(block=False)
                        logging.debug("Poison pill received")
                        kill_q.task_done()
                        return
                    except queue.Empty:
                        pass

                    # return if the limits of either iteration count and duration have been reached
                    if (iterations > 0 and c >= iterations) or \
                            (duration > 0 and time.time() >= endtime):
                        logging.debug("Task completed!")
                        # send poison pill
                        q.put(None)
                        return

                    # break if limit for connection duration has been reached
                    if conn_duration > 0 and time.time() >= conn_endtime:
                        logging.debug(
                            "conn_duration reached, will reset the connection.")
                        break

                    cycle_start = time.time()
                    for txn in w.txns:
                        start = time.time()
                        run_transaction(conn, lambda conn: txn(conn))
                        q.put((txn.__name__, time.time() - start))

                    c += 1
                    q.put(('__cycle__', time.time() - cycle_start))

        except psycopg.errors.UndefinedTable as e:
            q.put(e)
            return
        except psycopg.Error as e:
            logging.error(
                "Lost connection to the database. Sleeping for %s seconds." % (DEFAULT_SLEEP))
            time.sleep(DEFAULT_SLEEP)
        except Exception as e:
            logging.error("Exception: %s" % (e))


args = setup_parser()

# setup global logging
numeric_level = getattr(logging, args.loglevel.upper(), None)
if not isinstance(numeric_level, int):
    raise ValueError('Invalid log level: %s' % args.loglevel)

logging.basicConfig(level=numeric_level,
                    format='%(asctime)s [%(levelname)s] (%(processName)s %(process)d) %(message)s')


if __name__ == '__main__':
    main()
