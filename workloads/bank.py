import datetime as dt
import psycopg
import random
import time
import uuid
import logging


class Bank:
    def __init__(self, args: dict):
        # args is a dict of string passed with the --args flag
        # user passed a yaml/json, in python that's a dict object

        self.read_pct: float = float(args.get("read_pct", 50) / 100)

        self.lane: str = (
            random.choice(["ACH", "DEPO", "WIRE"])
            if not args.get("lane", "")
            else args["lane"]
        )

        # you can arbitrarely add any variables you want
        self.uuid: uuid.UUID = uuid.uuid4()
        self.ts: dt.datetime = ""
        self.event: str = ""

    # the setup() function is executed only once
    # when a new executing thread is started.
    def setup(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(f"select version()")
            print("setup >>>> ", cur.fetchone())

    # the run() function returns a list of functions
    # that pgworkload will execute, sequentially.
    # Once every func has been executed, run() is re-evaluated.
    # This process continues until pgworkload exits.
    def run(self):
        if random.random() < self.read_pct:
            return [self.read]
        return [self.txn1_new, self.txn2_verify, self.txn3_finalize]

    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def read(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                "select * from transactions where lane = %s and id = %s",
                (self.lane, self.uuid),
            )
            cur.fetchone()

    def txn1_new(self, conn: psycopg.Connection):
        # simulate microservice doing something
        self.uuid = uuid.uuid4()
        self.ts = dt.datetime.now()
        self.event = 0

        # make sure you pass the arguments in this fashion
        # so the statement can be PREPAREd (extended protocol).

        # Simple SQL strings will use the Simple Protocol.
        with conn.cursor() as cur:
            stmt = """
                insert into transactions values (%s, %s, %s, %s);
                """
            cur.execute(stmt, (self.lane, self.uuid, self.event, self.ts))

    # example on how to create a transaction with multiple queries
    def txn2_verify(self, conn: psycopg.Connection):
        # all queries sent within 'tx' will commit only when tx is exited
        with conn.transaction() as tx:
            with conn.cursor() as cur:
                # as we're inside 'tx', the below will not autocommit
                cur.execute(
                    "select * from ref_data where my_sequence = %s",
                    (random.randint(0, 100000),),
                )
                cur.fetchone()

                # simulate microservice doing something
                time.sleep(0.01)
                self.ts = dt.datetime.now()
                self.event = 1

                stmt = """
                    insert into transactions values (%s, %s, %s, %s);
                    """
                # as we're inside 'tx', the below will not autocommit
                cur.execute(stmt, (self.lane, self.uuid, self.event, self.ts))

    def txn3_finalize(self, conn: psycopg.Connection):
        with conn.transaction() as tx:
            with conn.cursor() as cur:
                cur.execute(
                    "select * from ref_data where my_sequence = %s",
                    (random.randint(0, 100000),),
                )
                cur.fetchone()

                # simulate microservice doing something
                self.ts = dt.datetime.now()
                self.event = 2
                time.sleep(0.01)

                stmt = """
                    insert into transactions values (%s, %s, %s, %s);
                    """
                cur.execute(stmt, (self.lane, self.uuid, self.event, self.ts))
