#!/usr/bin/python

import argparse
import logging
import pgworkload.models.run
import pgworkload.models.init
import pgworkload.models.util
import re
import sys
import os
import yaml
import json

DEFAULT_SLEEP = 5

logger = logging.getLogger(__name__)


def main():
    """This is the Entrypoint function
    """
    args: argparse.Namespace = setup_parser()

    logging.getLogger(__package__).setLevel(args.loglevel.upper())

    try:
        args.func(args)
    except AttributeError as e:
        logger.error(e)
        args.parser.print_help()
    except Exception as e:
        logger.error(e, stack_info=True)


def setup_parser():
    """Parses CLI arguments

    Returns:
        (argparse.Namespace): The object with the passed arguments
    """
    # Common options to all parsers
    common_parser = argparse.ArgumentParser(add_help=False)

    common_parser.add_argument('-l', '--log-level', dest='loglevel', default='INFO',
                               choices=['debug', 'info', 'warning', 'error'],
                               help='The log level ([debug|info|warning|error]). (default = info)')

    # root
    root = argparse.ArgumentParser(description='pgworkload - workload framework for the PostgreSQL protocol',
                                   epilog='GitHub: <https://github.com/fabiog1901/pgworkload>',
                                   parents=[common_parser])
    root.set_defaults(parser=root)
    root.set_defaults(func=print_help)

    root_sub = root.add_subparsers(help='')

    # workload options (common to root_init and root_run)
    workload_parser = argparse.ArgumentParser(add_help=False)

    workload_parser.add_argument('-w', '--workload', dest='workload_path', required=True,
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


def print_help(args: argparse.Namespace):
    args.parser.print_help()


def __validate(args: argparse.Namespace):
    """Performs pgworkload initialization steps

    Args:
        args (argparse.Namespace): args passed at the CLI

    Returns:
        argparse.Namespace: updated args
    """

    if not args.procs:
        args.procs = os.cpu_count()

    if not re.search(r'.*://.*/(.*)\?', args.dburl):
        logger.error(
            "The connection string needs to point to a database. Example: postgres://root@localhost:26257/postgres?sslmode=disable")
        sys.exit(1)

    workload = pgworkload.utils.util.import_class_at_runtime(
        path=args.workload_path)

    args.dburl = pgworkload.utils.util.set_query_parameter(url=args.dburl, param_name="application_name",
                                                           param_value=args.app_name if args.app_name else workload.__name__)

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
            logger.error(
                f"The value passed to '--args' is not a valid JSON or a valid path to a JSON/YAML file: '{args.args}'")
            sys.exit(1)

    return args


def run(args: argparse.Namespace):
    """Run the workload

    Args:
        args (argparse.Namespace): the args passed at the CLI
    """

    args = __validate(args)

    pgworkload.models.run.run(args)


def init(args: argparse.Namespace):
    """Initialize the workload.
    Includes tasks like:
    - create database and schema;
    - generate random datasets
    - import datasets into the database

    Args:
        args (argparse.Namespace): the args passed at the CLI
    """

    args = __validate(args)
    pgworkload.models.init.init(args)


def util_csv(args: argparse.Namespace):
    """Wrapper around SimpleFaker to create CSV datasets
    given an input YAML data gen definition file

    Args:
        args (argparse.Namespace): args passed at the CLI
    """
    pgworkload.models.util.util_csv(args)


def util_yaml(args: argparse.Namespace):
    """Wrapper around util function ddl_to_yaml() for 
    crafting a data gen definition YAML string from 
    CREATE TABLE statements.

    Args:
        args (argparse.Namespace): args passed at the CLI
    """

    pgworkload.models.util.util_yaml(args)
