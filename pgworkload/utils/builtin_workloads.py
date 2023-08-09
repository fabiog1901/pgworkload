import itertools
import psycopg


class Querybench:
    """Querybench runs the passed SQL statements
    sequentially and iteratively.

    The workload assumes the schema and data is pre-loaded.
    """

    def __init__(self, args: str):
        # organize all SQL stmts in individual strings
        if not args or not isinstance(args, str):
            raise ValueError("Parameter 'args' is empty or invalid.")

        # cleaning the input
        l: list = [x.strip() for x in args.split("\n")]
        l: list = " ".join([x for x in l if x != "" and not x.startswith("--")])

        self.stmts: list = [x.strip() for x in l[:-1].split(";")]

        # print(self.stmts)
        # create a continuous cycle from the parameters
        self.stmts_cycle = itertools.cycle(self.stmts)
        self.schema = ""
        self.load = ""

    def init(self, conn: psycopg.Connection):
        pass

    def run(self):
        return [self.txn for _ in self.stmts]

    def txn(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(next(self.stmts_cycle))


class Hovr:
    """Hovr is a spin-off of the original Movr workload by Cockroach Labs"""

    def __init__(self, args: dict):
        # self.schema holds the DDL
        self.schema: str = """
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
        self.load: str = """ 
# This has to be a YAML string so 
# it's important it starts with no indentation
# it's easier however, to pass a .yaml file instead
credits:
  - count: 2000
    sort-by:
      - id
    columns:
      id:
        type: UUIDv4
        args:
          seed: 0
"""

    def init(self, conn: psycopg.Connection):
        pass

    def run(self):
        return []

    def txn(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            pass
