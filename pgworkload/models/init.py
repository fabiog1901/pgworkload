#!/usr/bin/python

import datetime as dt
import logging
import multiprocessing as mp
import os
import pgworkload.utils.simplefaker
import pgworkload.utils.common
import psycopg
import sys

logger = logging.getLogger(__name__)


def init(
    db: str,
    workload_path: str,
    dburl: str,
    skip_schema: bool,
    drop: bool,
    skip_gen: bool,
    procs: int,
    csv_max_rows: int,
    skip_import: bool,
    http_server_hostname: str,
    http_server_port: str,
    args: dict,
    log_level: str,
):
    """Initialize the workload.
    Includes tasks like:
    - create database and schema;
    - generate random datasets
    - import datasets into the database
    """
    logger.setLevel(log_level)
    logger.debug("Running init")

    if not db:
        db = os.path.splitext(os.path.basename(workload_path))[0].lower()

    # PG or CRDB?
    try:
        dbms: str = pgworkload.utils.common.get_dbms(dburl)
    except ValueError as e:
        logger.error(e)
        sys.exit(1)
    except Exception as e:
        logger.error(e)
        dbms: str = None

    # PART 1 - CREATE THE SCHEMA
    if skip_schema:
        logger.debug("Skipping init_create_schema")
    else:
        __init_create_schema(dburl, drop, db, workload_path, dbms)

    # PART 2 - GENERATE THE DATA
    if skip_gen:
        logger.debug("Skipping init_generate_data")
    else:
        __init_generate_data(procs, workload_path, dbms, csv_max_rows)

    # PART 3 - IMPORT THE DATA
    dburl = pgworkload.utils.common.get_new_dburl(dburl, db)
    if skip_import:
        logger.debug("Skipping init_import_data")
    else:
        if not http_server_hostname:
            http_server_hostname = pgworkload.utils.common.get_hostname()
            logger.debug(f"Hostname identified as: '{http_server_hostname}'")

        __init_import_data(
            dburl, workload_path, dbms, http_server_hostname, http_server_port
        )

    # PART 4 - RUN WORKLOAD INIT
    logger.debug("Running workload.init()")
    workload = pgworkload.utils.common.import_class_at_runtime(workload_path)
    try:
        with psycopg.connect(dburl, autocommit=True) as conn:
            workload(args).init(conn)
    except Exception as e:
        logger.error(e)
        sys.exit(1)

    logger.info(
        "Init completed. Please update your database connection url to '%s'" % dburl
    )


def __init_create_schema(
    dburl: str, drop: bool, db_name: str, workload_path: str, dbms: str
):
    """Create database and schema

    Args:
        dburl (str): the default connection string to the database
        drop (bool): whether to drop the current database
        db_name (str): the name of the database for the workload
        workload_path (str): filepath to the workload class file
        dbms (str): DBMS technology (CockroachDB, PostgreSQL, etc..)
    """
    # create the database according to the value passed in --init-db,
    # or use the workload name otherwise.
    # drop any existant database if --init-drop is True
    logger.debug("Running init_create_schema")
    try:
        with psycopg.connect(dburl, autocommit=True) as conn:
            with conn.cursor() as cur:
                if drop:
                    logger.debug("Dropping database '%s'" % db_name)
                    if dbms == "CockroachDB":
                        cur.execute(
                            psycopg.sql.SQL(
                                "DROP DATABASE IF EXISTS {} CASCADE;"
                            ).format(psycopg.sql.Identifier(db_name))
                        )
                    elif dbms == "PostgreSQL":
                        cur.execute(
                            psycopg.sql.SQL("DROP DATABASE IF EXISTS {};").format(
                                psycopg.sql.Identifier(db_name)
                            )
                        )
                    else:
                        logger.error("DBMS not supported {dbms}")
                        sys.exit(1)

                # determine if database exists already
                # postgresql does not support CREATE DATABASE IF NOT EXISTS
                if (
                    cur.execute(
                        "SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,)
                    ).fetchone()
                    is None
                ):
                    logger.debug("Creating database '%s'" % db_name)
                    cur.execute(
                        psycopg.sql.SQL("CREATE DATABASE {};").format(
                            psycopg.sql.Identifier(db_name)
                        )
                    )

                logger.info("Database '%s' created." % db_name)

    except Exception as e:
        logger.error("Exception: %s" % (e))
        sys.exit(1)

    dburl = pgworkload.utils.common.get_new_dburl(dburl, db_name)

    # now that we've created the database, connect to that database
    # and load the schema, which can be in a <workload>.sql file
    # or in the self.schema variable of the workload.

    # find if the .sql file exists
    schema_sql_file = os.path.abspath(
        pgworkload.utils.common.get_based_name_dir(workload_path) + ".sql"
    )

    if os.path.exists(path=schema_sql_file):
        logger.debug("Found schema SQL file %s" % schema_sql_file)
        with open(schema_sql_file, "r") as f:
            schema = f.read()
    else:
        logger.debug(
            f"Schema file {schema_sql_file} not found. Loading schema from the 'schema' variable"
        )
        try:
            workload = pgworkload.utils.common.import_class_at_runtime(
                path=workload_path
            )
            schema = workload({}).schema
        except AttributeError as e:
            logger.error(f"{e}. Make sure self.schema is a valid variable in __init__")
            sys.exit(1)
    try:
        with psycopg.connect(conninfo=dburl, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(query=psycopg.sql.SQL(schema))

            logger.info("Created workload schema")

    except Exception as e:
        logger.error(f"Exception: {e}")
        sys.exit(1)


def __init_generate_data(procs: int, workload_path: str, dbms: str, csv_max_rows: int):
    """Generate random datasets for the workload using SimpleFaker.
    CSV files will be saved in a directory named after the workload.

    Args:
        procs (int): count of concurrent processes to be used to generate the datasets
        workload_path (str): filepath to the workload class
        dbms (str): DBMS technology (CockroachDB, PostgreSQL, etc..)
    """
    logger.debug("Running init_generate_data")
    # description of how to generate the data is in workload variable self.load

    load = pgworkload.utils.common.get_workload_load(workload_path)
    if not load:
        logger.info(
            "Data generation definition file (.yaml) or variable (self.load) not defined. Skipping"
        )
        return

    # get the dirname to put the csv files
    csv_dir: str = pgworkload.utils.common.get_based_name_dir(workload_path)

    # backup the current directory as to not override
    if os.path.isdir(csv_dir):
        os.rename(
            csv_dir, csv_dir + "." + dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        )

    # create new directory
    os.mkdir(csv_dir)

    # for CockroachDB, we want gzip files for faster network transfer
    compression = "gzip" if dbms == "CockroachDB" else None

    # generate the data by parsing the load variable
    pgworkload.utils.simplefaker.SimpleFaker(
        seed=0, csv_max_rows=csv_max_rows
    ).generate(load, procs, csv_dir, "\t", compression)


def __init_import_data(
    dburl: str,
    workload_path: str,
    dbms: str,
    http_server_hostname: str,
    http_server_port: str,
):
    """Import the datasets CSV files into the database

    Args:
        dburl (str): connection string to the database
        workload_path (str): filepath to the workload class
        dbms (str): DBMS technology (CockroachDB, PostgreSQL, etc..)
        http_server_hostname (str): The hostname of the server that serves the CSV files
        http_server_port (str): The port of the server that serves the CSV files
    """
    logger.debug("Running init_import_data")

    csv_dir = pgworkload.utils.common.get_based_name_dir(workload_path)
    load = pgworkload.utils.common.get_workload_load(workload_path)

    # Start the http server in a new Process
    mp.Process(
        target=pgworkload.utils.common.httpserver, args=(csv_dir, 3000), daemon=True
    ).start()

    if os.path.isdir(csv_dir):
        csv_files = os.listdir(csv_dir)
    else:
        logger.debug("Nothing to import, skipping...")
        return

    try:
        with psycopg.connect(dburl, autocommit=True) as conn:
            with conn.cursor() as cur:
                node_count = 1
                if dbms == "CockroachDB":
                    # fetch the count of nodes that are part of the cluster
                    cur.execute("select count(*) from crdb_internal.gossip_nodes;")
                    node_count = cur.fetchone()[0]

                for table in load.keys():
                    logger.info("Importing data for table '%s'" % table)
                    table_csv_files = [x for x in csv_files if x.split(".")[0] == table]

                    # chunked list is a list of list, where each item is a list of 'node_count' size.
                    chunked_list = [
                        table_csv_files[i : i + node_count]
                        for i in range(0, len(table_csv_files), node_count)
                    ]

                    # we import only 'node_count' items at a time, as
                    # we parallelize imports
                    for chunk in chunked_list:
                        if dbms == "CockroachDB":
                            stmt = pgworkload.utils.common.get_import_stmt(
                                chunk,
                                table.replace("__", "."),
                                http_server_hostname,
                                http_server_port,
                            )

                        elif dbms == "PostgreSQL":
                            stmt = "COPY %s FROM '%s';" % (
                                table,
                                os.path.join(os.getcwd(), chunk[0]),
                            )
                        else:
                            logger.warning(f"DBMS not supported: {dbms}")
                            pass

                        logger.debug(f"'Importing files using command: '{stmt}'")

                        cur.execute(stmt)

    except Exception as e:
        logger.error(f"Exception: {e}")
        sys.exit(1)
