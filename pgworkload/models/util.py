#!/usr/bin/python

import datetime as dt
import logging
import os
import pgworkload.utils.simplefaker
import pgworkload.utils.util
import yaml

logger = logging.getLogger(__name__)


def util_csv(
    input: str,
    output: str,
    compression: str,
    procs: int,
    csv_max_rows: int,
    delimiter: str,
    http_server_hostname: str,
    http_server_port: str,
    table_name: str,
):
    """Wrapper around SimpleFaker to create CSV datasets
    given an input YAML data gen definition file
    """

    with open(input, "r") as f:
        load = yaml.safe_load(f.read())

    if not output:
        output_dir = pgworkload.utils.util.get_based_name_dir(input)
    else:
        output_dir = output

    # backup the current directory as to not override
    if os.path.isdir(output_dir):
        os.rename(
            output_dir,
            output_dir + "." + dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S"),
        )

    # if the output dir is
    if os.path.exists(output_dir):
        output_dir += "_dir"

    # create new directory
    os.mkdir(output_dir)

    if not compression:
        compression = None

    if not procs:
        procs = os.cpu_count()

    pgworkload.utils.simplefaker.SimpleFaker(csv_max_rows=csv_max_rows).generate(
        load, int(procs), output_dir, delimiter, compression
    )

    csv_files = os.listdir(output_dir)

    if not http_server_hostname:
        http_server_hostname = pgworkload.utils.util.get_hostname()
        logger.debug(f"Hostname identified as: '{http_server_hostname}'")

    stmt = pgworkload.utils.util.get_import_stmt(
        csv_files, table_name, http_server_hostname, http_server_port
    )

    print(stmt)


def util_yaml(input: str, output: str):
    """Wrapper around util function ddl_to_yaml() for
    crafting a data gen definition YAML string from
    CREATE TABLE statements.
    """

    with open(input, "r") as f:
        ddl = f.read()

    if not output:
        output = pgworkload.utils.util.get_based_name_dir(input) + ".yaml"

    # backup the current file as to not override
    if os.path.exists(output):
        os.rename(output, output + "." + dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S"))

    # create new directory
    with open(output, "w") as f:
        f.write(pgworkload.utils.util.ddl_to_yaml(ddl))
