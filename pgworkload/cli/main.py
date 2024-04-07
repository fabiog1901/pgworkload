#!/usr/bin/python

from pgworkload.cli.dep import Param, EPILOG
from .. import __version__
from enum import Enum
from pathlib import Path
from typing import Optional
import json
import logging
import os
import pgworkload.cli.util
import pgworkload.models.init
import pgworkload.models.run
import pgworkload.models.util
import pgworkload.utils.common
import platform
import re
import sys
import typer
import yaml


logger = logging.getLogger(__name__)

app = typer.Typer(
    epilog=EPILOG,
    no_args_is_help=True,
    help=f"pgworkload v{__version__}: Workload utility for the PostgreSQL protocol.",
)


app.add_typer(pgworkload.cli.util.app, name="util")

version: bool = typer.Option(True)


class LogLevel(str, Enum):
    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"


@app.command(help="Run the workload.", epilog=EPILOG, no_args_is_help=True)
def run(
    workload_path: Optional[Path] = Param.WorkloadPath,
    builtin_workload: str = typer.Option(None, help="Built-in workload"),
    dburl: str = Param.DBUrl,
    procs: int = Param.Procs,
    args: str = Param.Args,
    concurrency: int = typer.Option(
        1, "-c", "--concurrency", help="Number of concurrent workers."
    ),
    ramp: int = typer.Option(0, "-r", "--ramp", help="Ramp up time in seconds."),
    iterations: int = typer.Option(
        None,
        "-i",
        "--iterations",
        help="Total number of iterations. Defaults to <ad infinitum>.",
        show_default=False,
    ),
    duration: int = typer.Option(
        None,
        "-d",
        "--duration",
        help="Duration in seconds. Defaults to <ad infinitum>.",
        show_default=False,
    ),
    conn_duration: int = typer.Option(
        None,
        "-k",
        "--conn-duration",
        show_default=False,
        help="The number of seconds to keep database connection alive before restarting. Defaults to <ad infinitum>.",
    ),
    app_name: Optional[str] = typer.Option(
        None,
        "--app-name",
        "-a",
        help="The application name specified by the client. Defaults to <db-name>.",
        show_default=False,
    ),
    autocommit: bool = typer.Option(
        True,
        "--no-autocommit",
        show_default=False,
        help="Unset autocommit in the connections.",
    ),
    frequency: int = typer.Option(
        10,
        "-s",
        "--stats-frequency",
        help="How often to display the stats in seconds. Set 0 to disable",
    ),
    prom_port: int = typer.Option(
        26260, "-p", "--port", help="The port of the Prometheus server."
    ),
    log_level: LogLevel = Param.LogLevel,
):
    logger.setLevel(log_level.upper())

    logger.debug("Executing run()")

    if workload_path:
        procs, dburl, args = __validate(procs, dburl, app_name, args, workload_path)

    else:
        procs, dburl, args = __validate(procs, dburl, app_name, args, builtin_workload)

    pgworkload.models.run.run(
        conc=concurrency,
        workload_path=workload_path,
        builtin_workload=builtin_workload,
        frequency=frequency,
        prom_port=prom_port,
        iterations=iterations,
        procs=procs,
        ramp=ramp,
        dburl=dburl,
        autocommit=autocommit,
        duration=duration,
        conn_duration=conn_duration,
        args=args,
        log_level=log_level.upper(),
    )


@app.command(help="Init the workload.", epilog=EPILOG, no_args_is_help=True)
def init(
    workload_path: Optional[Path] = Param.WorkloadPath,
    procs: int = Param.Procs,
    dburl: str = Param.DBUrl,
    drop: bool = typer.Option(False, "--drop", help="Drop the database if it exists."),
    csv_max_rows: int = Param.CSVMaxRows,
    skip_schema: bool = typer.Option(
        False, "-s", "--skip-schema", help="Don't run the schema creation script."
    ),
    skip_gen: bool = typer.Option(
        False, "-g", "--skip-gen", help="Don't generate the CSV data files."
    ),
    skip_import: bool = typer.Option(
        False, "-i", "--skip-import", help="Don't import the CSV data files."
    ),
    db: str = typer.Option(
        None, show_default=False, help="Override the default DB name."
    ),
    http_server_hostname: str = Param.HTTPServerHostName,
    http_server_port: int = Param.HTTPServerPort,
    args: str = Param.Args,
    log_level: LogLevel = Param.LogLevel,
):
    logging.getLogger(__package__).setLevel(log_level.upper())

    logger.debug("Executing run()")

    procs, dburl, args = __validate(procs, dburl, None, args, workload_path)

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
        args=args,
        log_level=log_level.upper(),
    )


def __validate(procs: int, dburl: str, app_name: str, args: str, workload_path: str):
    """Performs pgworkload initialization steps

    Args:
        args (argparse.Namespace): args passed at the CLI

    Returns:
        argparse.Namespace: updated args
    """

    workload = pgworkload.utils.common.import_class_at_runtime(workload_path)

    if not procs:
        procs = os.cpu_count()

    if not re.search(r".*://.*/(.*)\?", dburl):
        logger.error(
            "The connection string needs to point to a database. Example: postgres://root@localhost:26257/postgres?sslmode=disable"
        )
        sys.exit(1)

    dburl = pgworkload.utils.common.set_query_parameter(
        url=dburl,
        param_name="application_name",
        param_value=app_name if app_name else workload.__name__,
    )

    # load args dict from file or string
    if args:
        if os.path.exists(args):
            with open(args, "r") as f:
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
                    f"The value passed to '--args' is not a valid path to a JSON/YAML file, nor has no key:value pairs: '{args}'"
                )
                sys.exit(1)
    else:
        args = {}
    return procs, dburl, args


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"pgworkload : {__version__}")
        typer.echo(f"Python     : {platform.python_version()}")
        raise typer.Exit()


@app.callback()
def version_option(
    _: bool = typer.Option(
        False,
        "--version",
        "-v",
        callback=_version_callback,
        help="Print the version and exit",
    ),
) -> None:
    pass
