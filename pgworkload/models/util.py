#!/usr/bin/python

import argparse
import datetime as dt
import logging
import os
import pgworkload.utils.simplefaker
import pgworkload.utils.util
import sys
import time
import yaml

DEFAULT_SLEEP = 5


def signal_handler(sig, frame):
    """Handles Ctrl+C events gracefully, 
    ensuring all running processes are closed rather than killed.

    Args:
        sig (_type_): 
        frame (_type_): 
    """
    global stats
    global concurrency
    logging.info("KeyboardInterrupt signal detected. Stopping processes...")

    # send the poison pill to each worker
    for _ in range(concurrency):
        kill_q.put(None)

    # wait until all workers return
    start = time.time()
    c = 0
    timeout = True
    while c < concurrency and timeout:
        try:
            kill_q2.get(block=False)
            c += 1
        except:
            pass

        time.sleep(0.01)
        timeout = time.time() < start + 5

    if not timeout:
        logging.info("Timeout reached - forcing processes to stop")

    logging.info("Printing final stats")
    stats.print_stats()
    sys.exit(0)


def util_csv(args: argparse.Namespace):
    """Wrapper around SimpleFaker to create CSV datasets
    given an input YAML data gen definition file

    Args:
        args (argparse.Namespace): args passed at the CLI
    """
    if not args.input:
        logging.error("No input argument was passed")
        print()
        args.parser.print_help()
        sys.exit(1)

    with open(args.input, 'r') as f:
        load = yaml.safe_load(f.read())

    if not args.output:
        output_dir = pgworkload.utils.util.get_based_name_dir(args.input)
    else:
        output_dir = args.output

    # backup the current directory as to not override
    if os.path.isdir(output_dir):
        os.rename(output_dir, output_dir + '.' +
                  dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S'))

    # if the output dir is
    if os.path.exists(output_dir):
        output_dir += '_dir'

    # create new directory
    os.mkdir(output_dir)

    if not args.compression:
        args.compression = None

    if not args.procs:
        args.procs = os.cpu_count()

    pgworkload.utils.simplefaker.SimpleFaker(csv_max_rows=args.csv_max_rows).generate(
        load, int(args.procs), output_dir,  args.delimiter, args.compression)

    csv_files = os.listdir(output_dir)

    if not args.http_server_hostname:
        args.http_server_hostname = pgworkload.utils.util.get_hostname()
        logging.debug(
            f"Hostname identified as: '{args.http_server_hostname}'")

    stmt = pgworkload.utils.util.get_import_stmt(
        csv_files, args.table_name, args.http_server_hostname, args.http_server_port)

    print(stmt)


def util_yaml(args: argparse.Namespace):
    """Wrapper around util function ddl_to_yaml() for 
    crafting a data gen definition YAML string from 
    CREATE TABLE statements.

    Args:
        args (argparse.Namespace): args passed at the CLI
    """
    if not args.input:
        logging.error("No input argument was passed")
        print()
        args.parser.print_help()
        sys.exit(1)

    with open(args.input, 'r') as f:
        ddl = f.read()

    if not args.output:
        output = pgworkload.utils.util.get_based_name_dir(args.input) + '.yaml'
    else:
        output = args.output

    # backup the current file as to not override
    if os.path.exists(output):
        os.rename(output, output + '.' +
                  dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S'))

    # create new directory
    with open(output, 'w') as f:
        f.write(pgworkload.utils.util.ddl_to_yaml(ddl))


