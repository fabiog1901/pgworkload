import datetime
import itertools
import psycopg
import random
import string
import time
import uuid

class Retailer:

    def __init__(self, parameters=[]):
        self.parameters = parameters

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

        # self.load holds the dictionaries of functions to be executed to load the database tables
        self.load = """ 
credits:
  - count: 2000
    sort-by:
      - id
    tables:
      id:
        type: Integer
        args:
          start: 1
          end: 28
          seed: 0
      code:
        type: UUIDv4
        args:
          seed: 0
      channel:
        type: Choice
        args:
          population:
            - O
            - R
          weights:
            - 9
            - 1
          seed: 0
      pid:
        type: Integer
        args:
          start: 1
          end: 3572420
          seed: 0
      end_date:
        type: Date
        args:
          start: "2020-12-10"
          end: "2020-12-20"
          seed: 0
      status:
        type: Choice
        args:
          population:
            - A
            - R
          weights:
            - 99
            - 1
          seed: 0
      start_date:
        type: Date
        args:
          start: "2020-04-10"
          end: "2021-06-10"
          seed: 0
offers:
  - count: 2000
    sort-by: []
    tables:
      id:
        type: Integer
        args:
          start: 1
          end: 28
          seed: 0
      code:
        type: UUIDv4
        args:
          seed: 0
      token:
        type: UUIDv4
        args:
          seed: 1
      start_date:
        type: Date
        args:
          start: "2020-04-10"
          end: "2021-06-10"
          seed: 2
      end_date:
        type: Date
        args:
          start: "2020-04-10"
          end: "2021-06-10"
          seed: 3
  - count: 2000
    sort-by: []
    tables:
      id:
        type: Integer
        args:
          start: 1
          end: 28
          seed: 0
      code:
        type: UUIDv4
        args:
          seed: 0
      token:
        type: UUIDv4
        args:
          seed: 1
      start_date:
        type: Date
        args:
          start: "2020-04-10"
          end: "2021-06-10"
          seed: 2
      end_date:
        type: Date
        args:
          start: "2020-04-10"
          end: "2021-06-10"
          seed: 3
"""
            
    def init(self):
        pass
    
    def run(self):
        return [self.q1, self.q2]

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
            cur.execute(stmt)
