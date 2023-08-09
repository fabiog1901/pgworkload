#!/usr/bin/python

import http.server
import importlib
import logging
import numpy as np
import os
import pgworkload.utils.builtin_workloads
import psycopg
import random
import socket
import socketserver
import sys
import time
import urllib.parse
import yaml
import prometheus_client

RESERVED_WORDS = [
    "unique",
    "inverted",
    "index",
    "constraint",
    "family",
    "like",
    "primary",
    "foreign",
    "key",
    "create",
    "table",
    "if",
    "not",
    "exists",
    "null",
    "global",
    "local",
    "temporary",
    "temp",
    "unlogged",
    "visible",
    "using",
    "hash" "with",
    "bucket_count",
]

DEFAULT_ARRAY_COUNT = 3
SUPPORTED_DBMS = ["PostgreSQL", "CockroachDB"]


logger = logging.getLogger(__name__)


class QuietServerHandler(http.server.SimpleHTTPRequestHandler):
    """SimpleHTTPRequestHandler that doesn't output any log"""

    def log_message(self, format, *args):
        pass


class Stats:
    """Print workload stats
    and export the stats as Prometheus endpoints
    """

    def __init__(self, frequency: int, prom_port: int = 26260):
        self.cumulative_counts: dict[str, int] = {}
        self.instantiation_time = time.time()
        self.frequency = frequency

        self.prom_latency: dict[str, prometheus_client.Summary] = {}
        prometheus_client.start_http_server(prom_port)

        self.new_window()

    # reset stats while keeping cumulative counts
    def new_window(self) -> None:
        self.window_start_time: float = time.time()
        self.window_stats: dict[str, list[float]] = {}

    # add one latency measurement in seconds
    def add_latency_measurement(self, action: str, measurement: float) -> None:
        self.window_stats.setdefault(action, []).append(measurement)
        self.cumulative_counts.setdefault(action, 0)
        self.cumulative_counts[action] += 1

        if action not in self.prom_latency:
            self.prom_latency[action] = prometheus_client.Summary(
                f"latency_{action}", f"Latency for transaction {action}"
            )
        self.prom_latency[action].observe(measurement)

    # calculate the current stats this instance has collected.
    def calculate_stats(self) -> list:
        def get_stats_row(action: str):
            elapsed: float = time.time() - self.instantiation_time

            arr = np.array(self.window_stats[action])

            return [
                action,
                round(elapsed, 0),
                self.cumulative_counts[action],
                round(self.cumulative_counts[action] / elapsed, 2),
                len(arr),
                round(len(arr) / self.frequency, 2),
                round(np.mean(arr) * 1000, 2),
                round(np.percentile(arr, 50) * 1000, 2),
                round(np.percentile(arr, 90) * 1000, 2),
                round(np.percentile(arr, 95) * 1000, 2),
                round(np.percentile(arr, 99) * 1000, 2),
                round(np.max(arr) * 1000, 2),
            ]

        return [
            get_stats_row(action) for action in sorted(list(self.window_stats.keys()))
        ]


def set_query_parameter(url: str, param_name: str, param_value: str):
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
        workload = getattr(
            pgworkload.utils.builtin_workloads, path.lower().capitalize()
        )
        logger.info(f"Loading built-in workload '{path.lower().capitalize()}'")
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
        logger.error(e)
        sys.exit(1)
    except ImportError as e:
        logger.error(e)
        sys.exit(1)


def run_transaction(conn: psycopg.Connection, op, max_retries=3):
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
            logger.debug(f"psycopg.SerializationFailure:: {e}")
            conn.rollback()
            time.sleep((2**retry) * 0.1 * (random.random() + 0.5))
        except psycopg.Error as e:
            raise e

    raise ValueError(f"Transaction did not succeed after {max_retries} retries")


def get_based_name_dir(filepath: str):
    """Return the directory name based on the filename

    Args:
        filepath (str): the filepath, eg: /path/to/myfile.txt

    Returns:
        str: the name of the directory, eg: /path/to/file
    """
    return os.path.join(
        os.path.dirname(filepath),
        os.path.splitext(os.path.basename(filepath))[0].lower(),
    )


def get_workload_load(workload_path: str):
    """Get the data generation YAML string, as a Python dict object

    Args:
        workload_path (str): the workload class filepath

    Returns:
        (dict): the data gen definition
    """
    # find if the .yaml file exists
    yaml_file = os.path.abspath(get_based_name_dir(workload_path) + ".yaml")

    if os.path.exists(yaml_file):
        logger.debug("Found data generation definition YAML file %s" % yaml_file)
        with open(yaml_file, "r") as f:
            return yaml.safe_load(f)
    else:
        logger.debug(
            f"YAML file {yaml_file} not found. Loading data generation definition from the 'load' variable"
        )
        try:
            workload = import_class_at_runtime(workload_path)
            return yaml.safe_load(workload({}).load)
        except AttributeError as e:
            logger.warning(f"{e}. Make sure self.load is a valid variable in __init__")
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
    path = "/" + db_name
    return urllib.parse.urlunsplit((scheme, netloc, path, query_string, fragment))


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


def httpserver(path: str, port: int = 3000):
    """Create simple http server

    Args:
        path (string): The directory to serve files from
        port (int, optional): The http server listening port. Defaults to 3000.
    """
    os.chdir(path)

    try:
        with socketserver.TCPServer(
            server_address=("", port), RequestHandlerClass=QuietServerHandler
        ) as httpd:
            httpd.serve_forever()
    except OSError as e:
        logger.error(e)
        return


def get_hostname():
    """Get the hostname of the current host

    Returns:
        (str): the hostname
    """
    return socket.gethostname()


def ddl_to_yaml(ddl: str):
    """Transform a SQL DDL string of (multiple) CREATE TABLE statements
    into a data generation definition YAML string

    Args:
        ddl (str): CREATE TABLE statements

    Returns:
        (str): the YAML data gen definition string
    """

    def get_type_and_args(datatypes: list):
        # check if it is an array
        # string array
        # string []
        # string[]
        is_array = False
        datatypes = [x.lower() for x in datatypes]
        if "[]" in datatypes[0] or "array" in datatypes or "[]" in datatypes:
            is_array = True

        datatype: str = datatypes[0].replace("[]", "")

        if datatype.lower() in ["bool", "boolean"]:
            return {
                "type": "bool",
                "args": {
                    "seed": random.random(),
                    "null_pct": 0.0,
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() in [
            "int",
            "integer",
            "int2",
            "int4",
            "int8",
            "int64",
            "bigint",
            "smallint",
        ]:
            return {
                "type": "integer",
                "args": {
                    "min": 0,
                    "max": 1000000,
                    "seed": random.random(),
                    "null_pct": 0.0,
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() in ["string", "char", "character", "varchar", "text"]:
            return {
                "type": "string",
                "args": {
                    "min": 10,
                    "max": 30,
                    "seed": random.random(),
                    "null_pct": 0.0,
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() in [
            "decimal",
            "float",
            "dec",
            "numeric",
            "real",
            "double",
        ]:
            return {
                "type": "float",
                "args": {
                    "max": 10000,
                    "round": 2,
                    "seed": random.random(),
                    "null_pct": 0.0,
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() in ["time", "timetz"]:
            return {
                "type": "time",
                "args": {
                    "start": "07:30:00",
                    "end": "15:30:00",
                    "micros": False,
                    "seed": random.random(),
                    "null_pct": 0.0,
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() in ["json", "jsonb"]:
            return {
                "type": "jsonb",
                "args": {
                    "min": 10,
                    "max": 50,
                    "seed": random.random(),
                    "null_pct": 0.0,
                },
            }

        elif datatype.lower() == "date":
            return {
                "type": "date",
                "args": {
                    "start": "2022-01-01",
                    "end": "2022-12-31",
                    "format": "%Y-%m-%d",
                    "seed": random.random(),
                    "null_pct": 0.0,
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() in ["timestamp", "timestamptz"]:
            return {
                "type": "timestamp",
                "args": {
                    "start": "2022-01-01",
                    "end": "2022-12-31",
                    "format": "%Y-%m-%d %H:%M:%S.%f",
                    "seed": random.random(),
                    "null_pct": 0.0,
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() == "uuid":
            return {
                "type": "UUIDv4",
                "args": {
                    "seed": random.random(),
                    "null_pct": 0,
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        else:
            raise ValueError(f"Data type not implemented: '{datatype}'")

    def get_table_name_and_table_list(
        create_table_stmt: str, sort_by: list, count: int = 1000000
    ):
        p1 = create_table_stmt.find("(")
        p2 = create_table_stmt.rfind(")")

        # find table name (before parenthesis part)
        for i in create_table_stmt[:p1].split():
            if i.lower() not in RESERVED_WORDS:
                table_name = i.replace(".", "__")
                break

        # extract column definitions (within parentheses part)
        # eg:
        #   id uuid primary key
        #   s string(30)
        col_def_raw = create_table_stmt[p1 + 1 : p2]

        # remove slices delimited by parenthesis
        # eg: from id 'sting(30)' to 'id string'
        # this is important as within parenthesis we might find commas
        # and we need to split on commas later
        within_brackets = False
        col_def = ""
        for i in col_def_raw:
            if i == "(":
                within_brackets = True
                continue
            if i == ")":
                within_brackets = False
                continue
            if not within_brackets:
                col_def += i

        col_def = [x.strip().lower() for x in col_def.split(",")]

        ll = []
        for x in col_def:
            # remove commented lines
            if not x.startswith("--"):
                col_name_and_type = x.strip().split(" ")[:3]
                # remove those lines that are not column definition,
                # like CONSTRAINT, INDEX, FAMILY, etc..
                if col_name_and_type[0].lower() not in RESERVED_WORDS:
                    ll.append(col_name_and_type)

        table_list = []
        table_list.append({"count": count})
        table_list[0]["sort-by"] = sort_by
        table_list[0]["columns"] = {}

        for x in ll:
            table_list[0]["columns"][x[0]] = get_type_and_args(x[1:])

        return table_name, table_list

    def get_create_table_stmts(ddl: str):
        """Parses a DDL SQL file and returns only the CREATE TABLE stmts

        Args:
            ddl (str): the raw DDL string

        Returns:
            list: the list of CREATE TABLE stmts
        """

        # separate input into a 'create table' stmts list
        stmts = " ".join(x.lower() for x in ddl.split())

        # strip whitespace and remove empty items
        stmts: list = [x.strip() for x in stmts.split(";") if x != ""]

        # keep only string that start with 'create' and
        # have word 'table' between beginning and the first open parenthesis
        create_table_stmts = []
        for i in stmts:
            p1 = i.find("(")
            if i.startswith("create"):
                if "table" in i[:p1].lower():
                    create_table_stmts.append(i)

        return create_table_stmts

    stmts = get_create_table_stmts(ddl)

    d = {}
    for x in stmts:
        table_name, table_list = get_table_name_and_table_list(
            x, count=1000, sort_by=[]
        )
        d[table_name] = table_list

    return yaml.dump(d, default_flow_style=False, sort_keys=False)


def get_threads_per_proc(procs: int, threads: int):
    """Returns a list of threads count per procs

    Args:
        procs (int): procs count
        threads (int): threads count

    Returns:
        list: list of threads per procs
    """

    c = int(threads / procs)
    m = threads % procs

    l = [c for _ in range(min(procs, threads))]

    for x in range(m):
        l[x] += 1

    l.sort()

    return l


def get_import_stmt(
    csv_files: list,
    table_name: str,
    http_server_hostname: str = "myhost",
    http_server_port: str = "3000",
):
    csv_data = ""
    for x in csv_files:
        csv_data += "'http://%s:%s/%s'," % (http_server_hostname, http_server_port, x)

    stmt = "IMPORT INTO %s CSV DATA (%s) WITH delimiter = e'\\t', nullif = '';" % (
        table_name,
        csv_data[:-1],
    )

    return stmt
