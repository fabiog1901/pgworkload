#!/usr/bin/python

from io import TextIOWrapper
import datetime as dt
import logging
import os
import pgworkload.utils.simplefaker
import pgworkload.utils.common
import sys
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
        output_dir = pgworkload.utils.common.get_based_name_dir(input)
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
        http_server_hostname = pgworkload.utils.common.get_hostname()
        logger.debug(f"Hostname identified as: '{http_server_hostname}'")

    stmt = pgworkload.utils.common.get_import_stmt(
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
        output = pgworkload.utils.common.get_based_name_dir(input) + ".yaml"

    # backup the current file as to not override
    if os.path.exists(output):
        os.rename(output, output + "." + dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S"))

    # create new file
    with open(output, "w") as f:
        f.write(pgworkload.utils.common.ddl_to_yaml(ddl))


def util_merge(input_dir: str, output_dir: str, csv_max_rows: int):
    class Merge:
        def __init__(self, input_dir: str, output_dir: str, csv_max_rows: int):
            # input CSV files - it assumes files are already sorted
            files = os.listdir(input_dir)
            # Filtering only the files.
            self.CSVs = [
                os.path.join(input_dir, f)
                for f in files
                if os.path.isfile(os.path.join(input_dir, f))
            ]

            self.CSV_MAX_ROWS = csv_max_rows
            self.COUNTER = 0
            self.C = 0

            self.source: dict[int, list] = {}
            self.file_handlers: dict[int, TextIOWrapper] = {}
            self.output: TextIOWrapper
            if not output_dir:
                self.output_dir = str(input_dir) + ".merged"
            else:
                self.output_dir = output_dir

            # backup the current file as to not override
            if os.path.exists(self.output_dir):
                os.rename(
                    self.output_dir,
                    str(self.output_dir)
                    + "."
                    + dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S"),
                )

            # create new directory
            os.mkdir(self.output_dir)

        def initial_fill(self, csv: str, idx: int):
            """
            opens the CSV file, saves the file handler,
            read few lines into source list for the index.
            """
            f = open(csv, "r")
            self.file_handlers[idx] = f
            while len(self.source[idx]) < 5:
                line = f.readline()
                if line != "":
                    self.source[idx].append(line)
                else:
                    # reached end of file
                    logger.info(
                        f"initial_fill: CSV file '{csv}' at source index {idx} reached EOF."
                    )
                    f.close()
                    break

        def replenish_source_list(self, idx: int):
            """
            Refills the source list with a new value from the source file
            """
            try:
                f = self.file_handlers.get(idx, None)
                if not f:
                    return
                line = f.readline()
                if line != "":
                    self.source[idx].append(line)
                else:
                    # reached end of file
                    logger.info(f"index {idx} reached EOF.")
                    f.close()
                    del self.file_handlers[idx]
            except Exception as e:
                logger.error("Excepton in replenish_queue: ", e)

        def write_to_csv(self, v: str):
            if self.C >= self.CSV_MAX_ROWS:
                self.output.close()
                self.COUNTER += 1
                self.C = 0
                self.output = open(
                    os.path.join(
                        self.output_dir, f"out_{str.zfill(str(self.COUNTER), 3)}.csv"
                    ),
                    "+w",
                )

            self.output.write(v)
            self.C += 1

        def run(self):
            # init the source dict by opening each CSV file
            # and only reading few lines.
            for idx, csv in enumerate(self.CSVs):
                self.source[idx] = []

                self.initial_fill(csv, idx)

            # the source dict now has a key for every file and a list of the first values read

            l = []
            # pop the first value in each source to a list `l`
            # `l` will have the first values of all source CSV files
            for k, v in self.source.items():
                try:
                    l.append((v.pop(0), k))
                except IndexError as e:
                    pass

            first_k = None
            first_v = None
            self.output = open(
                os.path.join(
                    self.output_dir, f"out_{str.zfill(str(self.COUNTER), 3)}.csv"
                ),
                "+w",
            )

            # sort list `l`
            # pop the first value (the smallest) in `first_v`
            # make a note of the source of that value in `first_k`
            # replenish the corrisponding source
            while True:
                if first_k is not None:
                    try:
                        self.replenish_source_list(first_k)
                        l.append((self.source[first_k].pop(0), first_k))

                    except IndexError as e:
                        # the source list is empty
                        logger.info(f"source list {first_k} is now empty")
                        first_k = None

                if l:
                    l.sort(key=lambda x: x[0])
                    try:
                        first_v, first_k = l.pop(0)
                        self.write_to_csv(first_v)
                    except IndexError as e:
                        logger.info("Exception in main: ", e)
                        self.output.close()
                else:
                    break

            self.output.close()

            logger.info("Completed")

    Merge(input_dir, output_dir, csv_max_rows).run()
