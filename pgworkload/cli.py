import typer
import logging
import pgworkload.models.run
import pgworkload.models.init
import pgworkload.models.util
import pgworkload.utils.util
import re
import sys
import os
import yaml
import json
from typing import Optional
from pathlib import Path
from enum import Enum

import typer


class LogLevel(str, Enum):
    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"
    
# def main(config: Optional[Path] = typer.Option(None)):
#     if config is None:
#         print("No config file")
#         raise typer.Abort()
#     if config.is_file():
#         text = config.read_text()
#         print(f"Config file contents: {text}")
#     elif config.is_dir():
#         print("Config is a directory, will use all its config files")
#     elif not config.exists():
#         print("The config doesn't exist")


DEFAULT_SLEEP = 5

logger = logging.getLogger(__name__)

app = typer.Typer()
util_app = typer.Typer()
app.add_typer(util_app, name="util")


def __validate(procs: int, dburl: str, app_name: str, args: str, workload_path: str):
    """Performs pgworkload initialization steps

    Args:
        args (argparse.Namespace): args passed at the CLI

    Returns:
        argparse.Namespace: updated args
    """
    workload = pgworkload.utils.util.import_class_at_runtime(workload_path)

    if not procs:
        procs = os.cpu_count()

    if not re.search(r'.*://.*/(.*)\?', dburl):
        logger.error(
            "The connection string needs to point to a database. Example: postgres://root@localhost:26257/postgres?sslmode=disable")
        sys.exit(1)

    dburl = pgworkload.utils.util.set_query_parameter(url=dburl, param_name="application_name",
                                                      param_value=app_name if app_name else workload.__name__)

    # load args dict from file or string
    if args:
        if os.path.exists(args):
            with open(args, 'r') as f:
                args = f.read()
                # parse into JSON if it's a JSON string
                try:
                    args = json.load(args)
                except Exception as e:
                    pass
        else:
            args = yaml.safe_load(args)
            if isinstance(args, str):
                logger.error(
                    f"The value passed to '--args' is not a valid path to a JSON/YAML file, nor has no key:value pairs: '{args}'")
                sys.exit(1)
    else:
        args = {}
    return procs, dburl, args


@app.command(help='Run the workload')
def run(
        workload_path: Optional[Path] = typer.Option(
            ..., '--workload', '-w',
            help="Filepath to the Workload module.",
            exists=True,
            file_okay=True,
            dir_okay=False,
            writable=False,
            readable=True,
            resolve_path=True
            ),
        concurrency: int = typer.Option(
            1, '-c', "--concurrency", 
            help="Number of concurrent workers."),
        frequency: int = typer.Option(
            10, '-s', '--stats-frequency',
            help='How often to display the stats in seconds.'),
        prom_port: int = typer.Option(
            26260, '-p', '--port',
            help="The port of the Prometheus server."),
        iterations: int = typer.Option(
            None, '-i', '--iterations',
            help="Total number of iterations. Defaults to <ad infinitum>.", show_default=False),
        procs: int = typer.Option(
            None, '--procs', '-x',
            help="Number of Processes to use. Defaults to <system-cpu-count>.", show_default=False),
        ramp: int = typer.Option(
            0, '-r', '--ramp',
            help="Ramp up time in seconds."),
        dburl: str = typer.Option(
            'postgres://root@localhost:26257/postgres?sslmode=disable', '--url',
            help='The connection string to the database.'),
        app_name: Optional[str] = typer.Option(
            None, '--app-name', '-a',
            help='The application name specified by the client. Defaults to <db name>.', show_default=False),
        autocommit: bool = typer.Option(
            True, 
            help="Configure the psycopg connection with autocommit."),
        duration: int = typer.Option(
            None, '-d', '--duration', 
            help="Duration in seconds. Defaults to <ad infinitum>.", show_default=False),
        conn_duration: int = typer.Option(
            None, '-k', '--conn-duration', 
            show_default=False,
            help='The number of seconds to keep database connection alive before restarting. Defaults to <ad infinitum>.'),
        args: str = typer.Option(
            None, 
            help='JSON string, or filepath to a JSON/YAML file, to pass to Workload.'),
        log_level: LogLevel = typer.Option(
            'info', '--log-level', '-l', show_choices=True,
            help='Set the logging level.'
        )
    ):

    logging.getLogger(__package__).setLevel(log_level.upper())
    
    logger.debug("Executing run()")

    procs, dburl, args = __validate(
        procs, dburl, app_name, args, workload_path)

    pgworkload.models.run.run(
        conc=concurrency,
        workload_path=workload_path,
        frequency=frequency,
        prom_port=prom_port,
        iterations=iterations,
        procs=procs,
        ramp=ramp,
        dburl=dburl,
        autocommit=autocommit,
        duration=duration,
        conn_duration=conn_duration,
        args=args
    )


@app.command()
def init(
        workload_path: Optional[Path] = typer.Option(
            ..., '--workload', '-w',
            help="Filepath to the Workload module.",
            exists=True,
            file_okay=True,
            dir_okay=False,
            writable=False,
            readable=True,
            resolve_path=True
            ),
        procs: int = typer.Option(
            None, '--procs', '-x',
            help="Number of Processes to use. Defaults to <system-cpu-count>.", show_default=False),
        csv_max_rows: int = typer.Option(
            0, '-r', '--ramp',
            help="Ramp up time in seconds."),
        dburl: str = typer.Option(
            'postgres://root@localhost:26257/postgres?sslmode=disable', '--url',
            help='The connection string to the database.'),
        app_name: Optional[str] = typer.Option(
            None, '--app-name', '-a',
            help='The application name specified by the client. Defaults to <db name>.', show_default=False),
        drop: bool = typer.Option(
            True, 
            help="Configure the psycopg connection with autocommit."),
        skip_schema: bool = typer.Option(
            True, 
            help="Configure the psycopg connection with autocommit."),
        skip_gen: bool = typer.Option(
            True, 
            help="Configure the psycopg connection with autocommit."),
        skip_import: bool = typer.Option(
            True, 
            help="Configure the psycopg connection with autocommit."),
        db: int = typer.Option(
            None, '-d', '--duration', 
            help="Duration in seconds. Defaults to <ad infinitum>.", show_default=False),
        http_server_hostname: int = typer.Option(
            None, '-k', '--conn-duration', 
            show_default=False,
            help='The number of seconds to keep database connection alive before restarting. Defaults to <ad infinitum>.'),
        http_server_port: int = typer.Option(
            26260, '-p', '--port',
            help="The port of the Prometheus server."),
        args: str = typer.Option(
            None, 
            help='JSON string, or filepath to a JSON/YAML file, to pass to Workload.'),
        log_level: LogLevel = typer.Option(
            'info', '--log-level', '-l', show_choices=True,
            help='Set the logging level.'
        )
    ):

    logging.getLogger(__package__).setLevel(log_level.upper())
    
    logger.debug("Executing run()")
    
    procs, dburl, args = __validate(procs, dburl, app_name, args)

    pgworkload.models.init.init(
        db=db,
        workload_path=workload_path,
        dburl=dburl,
        skip_schema=skip_schema,
        drop=drop,
        skip_gen=skip_gen,
        procs=procs,
        csv_max_rows=csv_max_rows,
        skip_import=skip_import,
        http_server_hostname=http_server_hostname,
        http_server_port=http_server_port,
        args=args
    )


@util_app.command("csv")
def util_csv(input, output):
    pass
    # pgworkload.models.util.util_csv(
    #     input=args.input,
    #     output=args.output,
    #     compression=args.compression,
    #     procs=args.procs,
    #     csv_max_rows=args.csv_max_rows,
    #     delimiter=args.delimiter,
    #     http_server_hostname=args.http_server_hostname,
    #     http_server_port=args.http_server_port,
    #     table_name=args.table_name
    # )


@util_app.command("yaml")
def util_yaml(input, output):

    pgworkload.models.util.util_yaml(input=input, output=output)
