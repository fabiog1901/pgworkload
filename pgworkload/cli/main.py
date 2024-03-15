#!/usr/bin/python

from .. import __version__
from enum import Enum
from pathlib import Path
from typing import Optional
import json
import logging
import os
import pgworkload.models.init
import pgworkload.models.run
import pgworkload.models.util
import pgworkload.utils.util
import platform
import re
import sys
import typer
import yaml


EPILOG = "GitHub: <https://github.com/fabiog1901/pgworkload>"

logger = logging.getLogger(__name__)

app = typer.Typer(
    epilog=EPILOG,
    no_args_is_help=True,
    help=f"pgworkload v{__version__}: Workload utility for the PostgreSQL protocol.",
)

util_app = typer.Typer(
    epilog=EPILOG,
    no_args_is_help=True,
    help="Generate YAML data generation files and CSV datasets.",
)
app.add_typer(util_app, name="util")


version: bool = typer.Option(True)


class LogLevel(str, Enum):
    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"


class Param:
    LogLevel = typer.Option(
        "info", "--log-level", "-l", show_choices=True, help="Set the logging level."
    )

    WorkloadPath = typer.Option(
        None,
        "--workload",
        "-w",
        help="Filepath to the workload module.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
    )

    Procs = typer.Option(
        None,
        "--procs",
        "-x",
        help="Number of processes to spawn. Defaults to <system-cpu-count>.",
        show_default=False,
    )

    DBUrl = typer.Option(
        "postgres://root@localhost:26257/postgres?sslmode=disable",
        "--url",
        help="The connection string to the database.",
    )

    Args = typer.Option(
        None, help="JSON string, or filepath to a JSON/YAML file, to pass to Workload."
    )

    HTTPServerPort = typer.Option(
        3000,
        "-p",
        "--port",
        help="The port of the http server that servers the CSV files.",
    )

    HTTPServerHostName = typer.Option(
        None,
        "-n",
        "--hostname",
        show_default=False,
        help="The hostname of the http server that serves the CSV files.",
    )

    CSVMaxRows = typer.Option(100000, help="Max count of rows per resulting CSV file.")


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
        10, "-s", "--stats-frequency", help="How often to display the stats in seconds. Set 0 to disable"
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


@util_app.command(
    "csv",
    epilog=EPILOG,
    no_args_is_help=True,
    help="Generate CSV files from a YAML data generation file.",
)
def util_csv(
    input: Optional[Path] = typer.Option(
        ...,
        "--input",
        "-i",
        help="Filepath to the YAML data generation file.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        show_default=False,
        help="Output directory for the CSV files. Defaults to <input-basename>.",
        exists=False,
        file_okay=False,
        dir_okay=True,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
    procs: int = Param.Procs,
    csv_max_rows: int = Param.CSVMaxRows,
    http_server_hostname: str = Param.HTTPServerHostName,
    http_server_port: int = Param.HTTPServerPort,
    table_name: str = typer.Option(
        "table_name",
        "--table-name",
        "-t",
        help="The table name used in the import statement.",
    ),
    compression: str = typer.Option(
        None, "-c", "--compression", help="The compression format."
    ),
    delimiter: str = typer.Option(
        "\t",
        "-d",
        "--delimiter",
        help='The delimeter char to use for the CSV files. Defaults to "tab".',
        show_default=False,
    ),
):
    pgworkload.models.util.util_csv(
        input=input,
        output=output,
        compression=compression,
        procs=procs,
        csv_max_rows=csv_max_rows,
        delimiter=delimiter,
        http_server_hostname=http_server_hostname,
        http_server_port=http_server_port,
        table_name=table_name,
    )


@util_app.command(
    "yaml",
    epilog=EPILOG,
    no_args_is_help=True,
    help="Generate YAML data generation file from a DDL SQL file.",
)
def util_yaml(
    input: Optional[Path] = typer.Option(
        ...,
        "--input",
        "-i",
        help="Filepath to the DDL SQL file.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        show_default=False,
        help="Output filepath. Defaults to <input-basename>.yaml.",
        exists=False,
        file_okay=True,
        dir_okay=True,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
):
    pgworkload.models.util.util_yaml(input=input, output=output)


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

    if not re.search(r".*://.*/(.*)\?", dburl):
        logger.error(
            "The connection string needs to point to a database. Example: postgres://root@localhost:26257/postgres?sslmode=disable"
        )
        sys.exit(1)

    dburl = pgworkload.utils.util.set_query_parameter(
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
