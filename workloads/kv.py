import psycopg
import random
import time
import uuid

COL_TYPES = ["bytes", "uuid", "int", "string"]


class Kv:
    def __init__(self, args: dict):

        # ARGS
        self.think_time: float = float(args.get("think_time", 10) / 1000)
        self.batch_size: int = int(args.get("batch_size", 1))
        self.cycle_size: int = int(args.get("cycle_size", 100))
        self.table_name: str = args.get("table_name", "kv")
        self.key_size: int = int(args.get("key_size", 32))
        self.value_size: int = int(args.get("value_size", 256))
        self.key_type: str = args.get("key_type", "bytes")
        self.value_type: str = args.get("value_type", "bytes")

        # checks
        if self.key_type not in COL_TYPES:
            raise ValueError(
                f"The selected key_type '{self.key_type}' is invalid. The possible values are 'bytes', 'uuid', 'int', 'string'."
            )

        if self.value_type not in COL_TYPES:
            raise ValueError(
                f"The selected value_type '{self.key_type}' is invalid. The possible values are 'bytes', 'uuid', 'int', 'string'."
            )

        # make translation table from 0..255 to 97..122
        self.tbl = bytes.maketrans(
            bytearray(range(256)),
            bytearray(
                [ord(b"a") + b % 26 for b in range(113)]
                + [ord(b"0") + b % 10 for b in range(30)]
                + [ord(b"A") + b % 26 for b in range(113)]
            ),
        )

    def run(self):
        return [self.insert_kv, self.think] * self.cycle_size

    def think(self, conn: psycopg.Connection):
        time.sleep(self.think_time)

    def __get_data(self, data_type: str, size: int):
        if data_type == "bytes":
            return random.getrandbits(8 * size).to_bytes(size, "big")
        elif data_type == "uuid":
            return uuid.uuid4()
        elif data_type == "int":
            return random.randint(0, 2**63 - 1)
        elif data_type == "string":
            return (
                random.getrandbits(8 * size)
                .to_bytes(size, "big")
                .translate(self.tbl)
                .decode()
            )

    def insert_kv(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            args = []
            for _ in range(self.batch_size):
                args.extend(
                    [
                        self.__get_data(self.key_type, self.key_size),
                        self.__get_data(self.value_type, self.value_size),
                    ]
                )

            cur.execute(
                f"INSERT INTO {self.table_name} (k, v)  VALUES {'(%s, %s),' * self.batch_size}"[
                    :-1
                ],
                args,
            )
