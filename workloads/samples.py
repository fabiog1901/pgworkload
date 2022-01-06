import collections
import datetime
import faker
import itertools
import psycopg
import random
import string
import time
import uuid


class SimpleFaker():
    
    global fake
    fake = faker.Faker()
    
    def date_time_between(self, start: str = None, end: str = None, delta: int = 365, format: str = '%Y-%m-%d %H:%M:%S'):
        if start is None:
            start = datetime.date.today().isoformat()
        if end is None:
            end = (datetime.date.today() +
                   datetime.timedelta(days=delta)).isoformat()
        return fake.date_time_between(datetime.datetime.fromisoformat(start).date(), datetime.datetime.fromisoformat(end).date()).strftime(format)
    
    def random_choice(self, choices: list):
        return fake.random_element(collections.OrderedDict(choices))
    
    
class Retailer:

    def __init__(self, parameters=[]):

        # self.schema holds the DDL statement to create the schema
        self.schema = """
            CREATE TABLE IF NOT EXISTS credits (
                id INT2 NOT NULL,
                code UUID NOT NULL,
                channel STRING(1) NOT NULL,
                pid INT4 NOT NULL,
                end_date DATE NOT NULL,
                status STRING(1) NOT NULL,
                start_date DATE NOT NULL,
                CONSTRAINT "primary" PRIMARY KEY (id ASC, code ASC),
                INDEX credits_pid_idx (pid ASC),
                INDEX credits_code_id_idx (code ASC, id ASC) STORING (channel, status, end_date, start_date)
            );

            CREATE TABLE IF NOT EXISTS offers (
                id INT4 NOT NULL,
                code UUID NOT NULL,
                token UUID NOT NULL,
                start_date DATE,
                end_date DATE,
                CONSTRAINT "primary" PRIMARY KEY (id ASC, code ASC, token ASC),
                INDEX offers_token_idx (token ASC)
            );
            """

        # self.run holds the list of transactions to be executed in sequence
        self.run = [self.q1, self.q2]

        fake = faker.Faker()
        simplefake = SimpleFaker()
        rd = random.Random()

        '''
        # table credits ~7.5mio rows
        17,f5da34d7-6c8a-4c1c-af05-e09d41f9fca2,O,2223248,2020-12-10 02:05:14,A,2020-12-25 02:39:30

        int::start=1,end=28,seed=0; 
        uuid::seed=0; 
        
        choices::list=O R,weights=9 1,seed=0; 
        int::start=1,end=3572420,seed=0; 
        
        date::start=2020-12-15,delta=7,seed=0; 
        choices::list=A R,weights=99 1,seed=0; 
        date::start=2020-10-10,delta=180,seed=0

        ## OFFERS
        26,259f4329-e6f4-490b-9a16-4106cf6a659e,05b6e6e3-07d4-4edc-9143-1193e6c3f339,2021-09-16 21:42:15,2021-07-05 22:38:42

        carota -r 10000 -t "
        int::start=1,end=28,seed=0; 
        uuid::seed=0; 
        uuid::seed=1; 
        date; 
        date
        " -o o.csv
        
        # then we append some more random data
        carota -r 6000000 -t "int::start=0,end=100,seed=5; uuid::seed=5; uuid::seed=6; date; date" -o o.csv --append
        '''

        # self.load holds the dictionaries of functions to be executed to load the database tables
        self.load = {
            'credits': {
                'count': 10,
                'tables': {
                    'id': (fake.random_int, (1, 28)),
                    'code': (fake.uuid4, ()),
                    'channel': (simplefake.random_choice, ([('O', 9), ('R', 1)], )),
                    'pid': (fake.random_int, (1, 3572420)),
                    'end_date': (simplefake.date_time_between, ('2020-12-10', '2020-12-20')),
                    'status': (simplefake.random_choice, ([('A', 99), ('R', 1)], )),
                    'start_date': (simplefake.date_time_between, ('2020-07-01', '2021-01-31')),
                }
            },
            'offers': {
                'count': 10,
                'tables': {
                    'id': (fake.random_int, (1, 28)),
                    'code': (fake.uuid4, ()),
                    'token': (fake.uuid4, ()),
                    'start_date': (simplefake.date_time_between, ()),
                    'end_date': (simplefake.date_time_between, ('2020-07-01', '2021-01-31')),
                }
            }
        }

    def q1(self, conn: psycopg.Connection):

        with conn.cursor() as cur:
            stmt = """
            SELECT DISTINCT c.id, c.code, c.channel, c.status, c.end_date, c.start_date
            FROM credits AS c
            WHERE c.status = 'A'
                AND c.end_date >= '2020-11-20'
                AND c.start_date <= '2020-11-20'
                AND c.pid = '000000'

            UNION

            SELECT c.id, c.code, c.channel, c.status, c.end_date, c.start_date
            FROM credits AS c, offers AS o
            WHERE c.id = o.id
                AND c.code = o.code
                AND c.status = 'A'
                AND c.end_date >= '2020-11-20'
                AND c.start_date <= '2020-11-20'
                AND o.token = 'c744250a-1377-4cdf-a1f4-5b85a4d29aaa';
            """

            cur.execute(stmt)

    def q2(self, conn: psycopg.Connection):

        with conn.cursor() as cur:
            cur.execute("select * from bank where id = %s", (self.uuid,))
            cur.fetchone()

            # simulate microservice doing something
            time.sleep(0.005)

            stmt = """
            SELECT c.id, c.code, c.channel, c.status, c.end_date, c.start_date
            FROM credits AS c, offers AS o
            WHERE c.id = o.id
                AND c.code = o.code
                AND c.status = 'A'
                AND c.end_date >= '2020-11-20'
                AND c.start_date <= '2020-11-20'
                AND o.token = 'c744250a-1377-4cdf-a1f4-5b85a4d29aaa';
            """
            cur.execute(stmt, (self.uuid, self.event, self.lane, self.ts))


class Bank:

    def __init__(self, parameters=[]):
        # self.run holds the list of transactions to be executed in sequence
        self.run = [self.txn0, self.txn1, self.txn2]

        # you can arbitrarely add any variables you want
        self.uuid = ''
        self.ts = ''
        self.event = ''
        self.lane = parameters[0]

    def init(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            ddl = """CREATE DATABASE IF NOT EXISTS samples; 
                CREATE TABLE bank (
                    id UUID, 
                    event INT, 
                    lane STRING, 
                    ts TIMESTAMP, 
                    PRIMARY KEY (id, event)
                );
            """
            cur.execute(ddl)
        pass

    # conn is an instance of a psycopg connection object
    # conn is set with autocommit=True, so no need to send a commit message
    def txn0(self, conn: psycopg.Connection):
        self.uuid = uuid.uuid4()
        self.ts = datetime.datetime.now()
        self.event = 0

        with conn.cursor() as cur:
            stmt = """insert into bank values (%s, %s, %s, %s); 
                    """
            cur.execute(stmt, (self.uuid, self.event, self.lane, self.ts))

    # example on how to create a transaction with multiple queries
    def txn1(self, conn: psycopg.Connection):
        self.ts = datetime.datetime.now()
        self.event = 1

        with conn.transaction() as tx:
            with conn.cursor() as cur:
                cur.execute("select * from bank where id = %s", (self.uuid,))
                cur.fetchone()

                # simulate microservice doing something
                time.sleep(0.005)

                stmt = """insert into bank values (%s, %s, %s, %s); 
                        """
                cur.execute(stmt, (self.uuid, self.event, self.lane, self.ts))

    def txn2(self, conn: psycopg.Connection):
        self.ts = datetime.datetime.now()
        self.event = 2

        with conn.transaction() as tx:
            with conn.cursor() as cur:
                cur.execute("select * from bank where id = %s", (self.uuid,))
                cur.fetchone()

                # simulate microservice doing something
                time.sleep(0.010)

                stmt = """insert into bank values (%s, %s, %s, %s); 
                        """
                cur.execute(stmt, (self.uuid, self.event, self.lane, self.ts))


class Balance:

    def __init__(self, parameters=[]):
        # create a list of transactions with as many elements as parameters
        self.run = [self.insert_balance for x in parameters]
        # create a continuous cycle from the parameters
        self.row_cycle = itertools.cycle(parameters)

        self.id = uuid.uuid4()
        self.corr = ''.join(random.choice(string.ascii_uppercase)
                            for x in range(4))
        self.system_dt = datetime.datetime.now()
        self.office = random.choice(
            ['LDN', 'TKO', 'NYC', 'SGP'])
        self.acct_no = str(random.randint(1000000, 12000000))
        self.sub_acct_no = str(random.randint(1000000, 12000000))
        self.acct_type = random.choice(['ch', 'sv', 'mg', 'ln'])
        self.symbol = ''.join(random.choice(string.ascii_uppercase)
                              for x in range(25))
        self.sym_no = random.randint(1, 100)
        self.price = round(random.random() * 100000, 2)
        self.topen = round(random.random() * 100000, 2)
        self.tclose = round(random.random() * 100000, 2)
        self.tmktval = round(random.random() * 100000, 2)
        self.sopen = round(random.random() * 100000, 2)
        self.sclose = round(random.random() * 100000, 2)
        self.smktval = round(random.random() * 100000, 2)
        self.seg_orig = round(random.random() * 100000, 2)
        self.seg_qty = round(random.random() * 100000, 2)
        self.seg_fluid = round(random.random() * 100000, 2)
        self.memo_rights = round(random.random() * 100000, 2)
        self.memo_tender = round(random.random() * 100000, 2)
        self.memo_splits = round(random.random() * 100000, 2)
        self.memo_merger = round(random.random() * 100000, 2)
        self.memo_acats = round(random.random() * 100000, 2)
        self.memo_transfer = round(random.random() * 100000, 2)
        self.memo_safekeep = round(random.random() * 100000, 2)
        self.ex_req_value = round(random.random() * 100000, 2)
        self.ho_req_value = round(random.random() * 100000, 2)
        self.ex_req_method = ''.join(random.choice(
            string.ascii_uppercase) for x in range(10))
        self.exec_symbol = ''.join(random.choice(
            string.ascii_uppercase) for x in range(25))
        self.g_tcost = round(random.random() * 100000, 2)
        self.n_tcost = round(random.random() * 100000, 2)
        self.memo_firmuse = round(random.random() * 100000, 2)
        self.fed_req_value = round(random.random() * 100000, 2)
        self.hold_type = ''.join(random.choice(
            string.ascii_uppercase) for x in range(1))
        self.seg_earlyrel = round(random.random() * 100000, 2)
        self.factor = round(random.random() * 100000, 2)
        self.factor_dt = datetime.datetime.today()

    def reset(self):
        return (
            # id
            uuid.uuid4(),

            # corr
            ''.join(random.choice(string.ascii_uppercase) for x in range(4)),

            # system_dt
            datetime.datetime.now(),

            # office
            random.choice(['LDN', 'TKO', 'NYC', 'SGP']),

            # acct_no
            str(random.randint(1000000, 12000000)),

            # sub_acct_no
            self.sub_acct_no,
            # str(random.randint(1000000, 12000000)),

            # acct_type
            random.choice(['ch', 'sv', 'mg', 'ln']),

            # symbol
            ''.join(random.choice(string.ascii_uppercase) for x in range(25)),

            # sym_no
            random.randint(1, 10000),

            # price
            round(random.random() * 100000, 2),

            # topen
            round(random.random() * 100000, 2),

            # tclose
            round(random.random() * 100000, 2),

            # tmktval
            round(random.random() * 100000, 2),

            # sopen
            round(random.random() * 100000, 2),

            # sclose
            round(random.random() * 100000, 2),

            # smktval
            round(random.random() * 100000, 2),

            # seg_orig
            round(random.random() * 100000, 2),

            # seg_qty
            round(random.random() * 100000, 2),

            # seg_fluid
            round(random.random() * 100000, 2),

            # memo_rights
            round(random.random() * 100000, 2),

            # memo_tender
            round(random.random() * 100000, 2),

            # memo_splits
            round(random.random() * 100000, 2),

            # memo_merger
            round(random.random() * 100000, 2),

            # memo_acats
            self.memo_acats,
            # round(random.random() * 100000, 2),

            # memo_transfer
            self.memo_transfer,
            # round(random.random() * 100000, 2),

            # memo_safekeep
            round(random.random() * 100000, 2),

            # ex_req_value
            round(random.random() * 100000, 2),

            # ho_req_value
            round(random.random() * 100000, 2),

            # ex_req_method
            self.ex_req_method,
            # ''.join(random.choice(string.ascii_uppercase) for x in range(10)),

            # exec_symbol
            self.exec_symbol,
            # ''.join(random.choice(string.ascii_uppercase) for x in range(25)),

            # g_tcost
            round(random.random() * 100000, 2),

            # n_tcost
            round(random.random() * 100000, 2),

            # memo_firmuse
            self.memo_firmuse,
            # round(random.random() * 100000, 2),

            # fed_req_value
            round(random.random() * 100000, 2),

            # hold_type
            random.choice(string.ascii_uppercase),

            # seg_earlyrel
            self.seg_earlyrel,
            # round(random.random() * 100000, 2),

            # factor
            self.factor,
            # round(random.random() * 100000, 2),

            # factor_dt
            datetime.datetime.today()
        )

    def init(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            ddl = """
            CREATE TABLE balance (
                id UUID NOT NULL DEFAULT gen_random_uuid(),
                corr STRING(4) NOT NULL,
                system_dt TIMESTAMP NOT NULL,
                office STRING(4) NOT NULL,
                acct_no STRING(12) NOT NULL,
                sub_acct_no STRING(12) NULL,
                acct_type STRING(2) NOT NULL,
                symbol STRING(25) NOT NULL,
                sym_no INT NOT NULL DEFAULT 0,
                price DECIMAL(14,6) NOT NULL,
                topen DECIMAL(16,6) NULL,
                tclose DECIMAL(19,8) NOT NULL,
                tmktval DECIMAL(19,6) NULL,
                sopen DECIMAL(16,6) NULL,
                sclose DECIMAL(19,8) NOT NULL,
                smktval DECIMAL(19,6) NULL,
                seg_orig DECIMAL(19,8) NULL,
                seg_qty DECIMAL(19,8) NULL,
                seg_fluid DECIMAL(16,6) NULL,
                memo_rights DECIMAL(16,6) NULL,
                memo_tender DECIMAL(16,6) NULL,
                memo_splits DECIMAL(16,6) NULL,
                memo_merger DECIMAL(16,6) NULL,
                memo_acats DECIMAL(16,6) NULL,
                memo_transfer DECIMAL(16,6) NULL,
                memo_safekeep DECIMAL(16,6) NULL,
                ex_req_value DECIMAL(19,6) NULL,
                ho_req_value DECIMAL(19,6) NULL,
                ex_req_method STRING(10) NULL,
                exec_symbol STRING(25) NULL,
                g_tcost DECIMAL(19,6) NULL,
                n_tcost DECIMAL(19,6) NULL,
                memo_firmuse DECIMAL(16,6) NULL,
                fed_req_value DECIMAL(19,6) NULL,
                hold_type STRING NOT NULL DEFAULT 'L':::STRING,
                seg_earlyrel DECIMAL(19,8) NULL,
                factor DECIMAL(19,12) NULL,
                factor_dt DATE NULL,
                CONSTRAINT "primary" PRIMARY KEY (id ASC),
                UNIQUE INDEX balance_i2 (acct_no ASC, system_dt ASC, corr ASC, office ASC,
                acct_type ASC, sym_no ASC, hold_type ASC),
                INDEX balance_i3 (sym_no ASC, system_dt ASC),
                FAMILY "primary" (id, corr, system_dt, office, acct_no, sub_acct_no,
                acct_type, symbol, sym_no, price, topen, tclose, tmktval, sopen, sclose,
                smktval, seg_orig, seg_qty, seg_fluid, memo_rights, memo_tender, memo_splits,
                memo_merger, memo_acats, memo_transfer, memo_safekeep, ex_req_value,
                ho_req_value, ex_req_method, exec_symbol, g_tcost, n_tcost, memo_firmuse,
                fed_req_value, hold_type, seg_earlyrel, factor, factor_dt)
            );
            """
            cur.execute(ddl)

    def next_row(self):
        return int(next(self.row_cycle))

    def insert_balance(self, conn: psycopg.Connection):

        stmt = "INSERT INTO balance VALUES "
        stmt_args = ()

        for _ in range(self.next_row()):
            stmt += """
            (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s
            ),"""

            stmt_args += self.reset()

        with conn.cursor() as cur:
            cur.execute(stmt[:-1], stmt_args)
