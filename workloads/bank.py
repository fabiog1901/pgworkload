import datetime as dt
import psycopg
import random
import time
import uuid


class Bank:

    def __init__(self, parameters: list):
        # parameters is a list of string passed with the --parameters flag
        self.parameters = parameters

        # in this case, item at index 0 holds the % of read ops
        self.read_pct = float(self.parameters[0]) / 100
        # the second item holds the string for the lane
        self.lane = parameters[1]

        # self.schema holds the DDL
        self.schema = """
            -- you can write the schema ddl here, but it's simpler to pass a .sql file
            CREATE TABLE IF NOT EXISTS transaction (
                id UUID,
                event INT,
                lane STRING,
                ts TIMESTAMP,
                PRIMARY KEY (id, event)
            );
            """

        # self.load holds the dictionaries of functions to be executed
        # to load the database tables
        self.load = """ 
# This has to be a YAML string so 
# it's important it starts with no indentation
# it's easier however, to pass a .yaml file instead
credits:
  - count: 2000
    sort-by:
      - id
    tables:
      id:
        type: UUIDv4
        args:
          seed: 0
"""

        # you can arbitrarely add any variables you want
        self.uuid = uuid.uuid4()
        self.ts = ''
        self.event = ''


    # the 'init' method is executed once, when the --init flag is passed
    def init(self):
        pass

    # the run method returns a list of transactions to be executed continuosly,
    # sequentially, as in a cycle.
    def run(self):
        if random.random() < self.read_pct:
            return [self.read]
        return [self.txn1_new, self.txn2_verify, self.txn3_finalize]

    # conn is an instance of a psycopg connection object
    # conn is set with autocommit=True, so no need to send a commit message
    def read(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute("select * from transactions where id = %s", (self.uuid, ))
            cur.fetchone()

    def txn1_new(self, conn: psycopg.Connection):
        # simulate microservice doing something
        self.uuid = uuid.uuid4()
        self.ts = dt.datetime.now()
        self.event = 0

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
                    "select * from ref_data where my_sequence = %s", (random.randint(0, 100000), ))
                cur.fetchone()

                # simulate microservice doing something
                time.sleep(0.005)
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
                    "select * from ref_data where my_sequence = %s", (random.randint(0, 100000), ))
                cur.fetchone()

                # simulate microservice doing something
                self.ts = dt.datetime.now()
                self.event = 2
                time.sleep(0.010)

                stmt = """
                    insert into transactions values (%s, %s, %s, %s);
                    """
                cur.execute(stmt, (self.lane, self.uuid, self.event, self.ts))
