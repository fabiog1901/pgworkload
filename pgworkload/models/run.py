#!/usr/bin/python

import logging
import multiprocessing as mp
import pgworkload.utils.common
import psycopg
import queue
import random
import signal
import sys
import threading
import time
import traceback
import logging.handlers
import tabulate

DEFAULT_SLEEP = 3

logger = logging.getLogger(__name__)

HEADERS: list = [
    "id",
    "elapsed",
    "tot_ops",
    "tot_ops/s",
    "period_ops",
    "period_ops/s",
    "mean(ms)",
    "p50(ms)",
    "p90(ms)",
    "p95(ms)",
    "p99(ms)",
    "pMax(ms)",
]


def signal_handler(sig, frame):
    """Handles Ctrl+C events gracefully,
    ensuring all running processes are closed rather than killed.

    Args:
        sig (_type_):
        frame (_type_):
    """
    global stats
    global concurrency
    logger.info("KeyboardInterrupt signal detected. Stopping processes...")

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
        logger.info("Timeout reached - forcing processes to stop")

    logger.info("Printing final stats")
    __print_stats()
    sys.exit(0)


def __ramp_up(processes: list, ramp_interval: int):
    for p in processes:
        logger.info("Starting a new Process...")
        p.start()
        time.sleep(ramp_interval)


def run(
    conc: int,
    workload_path: str,
    builtin_workload: str,
    frequency: int,
    prom_port: int,
    iterations: int,
    procs: int,
    ramp: int,
    dburl: str,
    autocommit: bool,
    duration: int,
    conn_duration: int,
    args: dict,
    log_level: str,
):
    logger.setLevel(log_level)

    global stats
    global concurrency
    global kill_q
    global kill_q2

    concurrency = conc

    workload = pgworkload.utils.common.import_class_at_runtime(
        workload_path if workload_path else builtin_workload
    )

    signal.signal(signal.SIGINT, signal_handler)

    disable_stats = False

    if frequency == 0:
        disable_stats = True
        frequency = 10

    stats = pgworkload.utils.common.Stats(frequency, prom_port)

    if iterations:
        iterations = iterations // concurrency

    q = mp.Queue(maxsize=1000)
    kill_q = mp.Queue()
    kill_q2 = mp.Queue()

    c = 0

    threads_per_proc = pgworkload.utils.common.get_threads_per_proc(procs, conc)
    ramp_interval = int(ramp / len(threads_per_proc))

    processes: list[mp.Process] = []

    for x in threads_per_proc:
        processes.append(
            mp.Process(
                target=worker,
                args=(
                    x - 1,
                    q,
                    kill_q,
                    kill_q2,
                    log_level,
                    dburl,
                    autocommit,
                    workload,
                    args,
                    iterations,
                    duration,
                    conn_duration,
                    disable_stats,
                ),
            )
        )

    threading.Thread(
        target=__ramp_up, daemon=True, args=(processes, ramp_interval)
    ).start()

    try:
        stat_time = time.time() + frequency
        while True:
            try:
                # read from the queue for stats or completion messages
                tup = q.get(block=False)
                if isinstance(tup, tuple):
                    stats.add_latency_measurement(*tup)
                else:
                    c += 1
            except queue.Empty:
                pass

            if c >= concurrency:
                if isinstance(tup, psycopg.errors.UndefinedTable):
                    logger.error(tup)
                    logger.error(
                        "The schema is not present. Did you initialize the workload?"
                    )
                    sys.exit(1)
                elif isinstance(tup, Exception):
                    logger.error("Exception raised: %s" % tup)
                    sys.exit(1)
                else:
                    logger.info(
                        "Requested iteration/duration limit reached. Printing final stats"
                    )
                    __print_stats()

                    sys.exit(0)

            if time.time() >= stat_time:
                __print_stats()
                stats.new_window()
                stat_time = time.time() + frequency

    except Exception as e:
        logger.error(traceback.format_exc())


def worker(
    thread_count: int,
    q: mp.Queue,
    kill_q: mp.Queue,
    kill_q2: mp.Queue,
    log_level: str,
    dburl: str,
    autocommit: bool,
    workload: object,
    args: dict,
    iterations: int,
    duration: int,
    conn_duration: int,
    disable_stats: bool,
):
    """Process worker function to run the workload in a multiprocessing env

    Args:
        thread_count(int): The number of threads to create
        q (mp.Queue): queue to report query metrics
        kill_q (mp.Queue): queue to handle stopping the worker
        kill_q2 (mp.Queue): queue to handle stopping the worker
        dburl (str): connection string to the database
        autocommit (bool): whether to set autocommit for the connection
        workload (object): workload class object
        args (dict): args to init the workload class
        iterations (int): count of workload iteration before returning
        duration (int): seconds before returning
        conn_duration (int): seconds before restarting the database connection
        disable_stats: (bool): flag to send or not stats back to the mainthread
    """
    logger.setLevel(log_level)

    threads: list[threading.Thread] = []

    for _ in range(thread_count):
        thread = threading.Thread(
            target=worker,
            daemon=True,
            args=(
                0,
                q,
                kill_q,
                kill_q2,
                log_level,
                dburl,
                autocommit,
                workload,
                args,
                iterations,
                duration,
                conn_duration,
                disable_stats,
            ),
        )
        thread.start()
        threads.append(thread)

    if threading.current_thread().name == "MainThread":
        # capture KeyboardInterrupt and do nothing
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    # catch exception while instantiating the workload class
    try:
        w = workload(args)
    except Exception as e:
        stack_lines = traceback.format_exc()
        q.put(Exception(stack_lines))
        return

    c = 0
    endtime = 0
    conn_endtime = 0

    if duration:
        endtime = time.time() + duration

    while True:
        if conn_duration:
            # reconnect every conn_duration +/- 20%
            conn_endtime = time.time() + int(conn_duration * random.uniform(0.8, 1.2))
        # listen for termination messages (poison pill)
        try:
            kill_q.get(block=False)
            logger.debug("Poison pill received")
            kill_q2.put(None)
            for x in threads:
                x.join()

            return
        except queue.Empty:
            pass
        try:
            with psycopg.connect(dburl, autocommit=autocommit) as conn:
                logger.debug("Connection started")
                while True:
                    # listen for termination messages (poison pill)
                    try:
                        kill_q.get(block=False)
                        logger.debug("Poison pill received")
                        kill_q2.put(None)
                        for x in threads:
                            x.join()
                        return
                    except queue.Empty:
                        pass

                    # return if the limits of either iteration count and duration have been reached
                    if (iterations and c >= iterations) or (
                        duration and time.time() >= endtime
                    ):
                        logger.debug("Task completed!")

                        # send task completed notification (a None)
                        q.put(None)
                        for x in threads:
                            x.join()
                        return

                    # break from the inner loop if limit for connection duration has been reached
                    # this will cause for the outer loop to reset the timer and restart with a new conn
                    if conn_duration and time.time() >= conn_endtime:
                        logger.debug(
                            "conn_duration reached, will reset the connection."
                        )
                        break

                    cycle_start = time.time()
                    for txn in w.run():
                        start = time.time()
                        pgworkload.utils.common.run_transaction(
                            conn, lambda conn: txn(conn)
                        )
                        if not q.full() and not disable_stats:
                            q.put((txn.__name__, time.time() - start))

                    c += 1
                    if not q.full() and not disable_stats:
                        q.put(("__cycle__", time.time() - cycle_start))

        # catch any error, pass that error to the MainProcess
        except psycopg.errors.UndefinedTable as e:
            q.put(e)
            return
        # psycopg.OperationalErrors can either mean a disconnection
        # or some other errors.
        # We don't stop if a node goes doesn, instead, wait few seconds and attempt
        # a new connection.
        # If the error is not beacuse of a disconnection, then unfortunately
        # the worker will continue forever
        except psycopg.Error as e:
            logger.error(f"{e.__class__.__name__} {e}")
            logger.info("Sleeping for %s seconds" % (DEFAULT_SLEEP))
            time.sleep(DEFAULT_SLEEP)
        except Exception as e:
            logger.error("Exception: %s" % (e), stack_info=True)
            q.put(e)
            return


def __print_stats():
    print(tabulate.tabulate(stats.calculate_stats(), HEADERS), "\n")
