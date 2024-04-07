#!/usr/bin/python

from .. import __version__
import typer

EPILOG = "GitHub: <https://github.com/fabiog1901/pgworkload>"


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
