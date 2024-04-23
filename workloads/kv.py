from collections import deque
import psycopg
import random
import time
import uuid

COL_TYPES = ["bytes", "uuid", "int", "string"]
WRITE_MODES = ["insert", "upsert", "do_nothing"]


class Kv:
    def __init__(self, args: dict):
        # ARGS
        self.think_time: float = float(args.get("think_time", 10) / 1000)
        self.batch_size: int = int(args.get("batch_size", 1))
        self.cycle_size: int = int(args.get("cycle_size", 1))
        self.table_name: str = args.get("table_name", "kv")
        self.key_size: int = int(args.get("key_size", 32))
        self.value_sizes: list = [
            int(x) if x else None
            for x in str(args.get("value_sizes", "256")).split(",")
        ]
        self.key_type: str = args.get("key_type", "bytes")
        self.value_types: list = args.get("value_types", "bytes").split(",")
        self.seed: str = args.get("seed", None)
        self.read_pct: float = float(args.get("read_pct", 0) / 100)
        self.update_pct: float = self.read_pct + float(args.get("update_pct", 0) / 100)
        self.delete_pct: float = self.update_pct + float(
            args.get("delete_pct", 0) / 100
        )
        self.key_pool_size: int = int(args.get("key_pool_size", 10000))
        self.write_mode: str = args.get("write_mode", "insert")
        self.aost: str = args.get("aost", "")

        # type checks
        if self.key_type not in COL_TYPES:
            raise ValueError(
                f"The selected key_type '{self.key_type}' is invalid. The possible values are 'bytes', 'uuid', 'int', 'string'."
            )

        for t in self.value_types:
            if t not in COL_TYPES:
                raise ValueError(
                    f"The selected value_type '{t}' is invalid. The possible values are 'bytes', 'uuid', 'int', 'string'."
                )

        self.value_types_and_sizes = dict(zip(self.value_types, self.value_sizes))

        # write_mode checks
        if self.write_mode not in WRITE_MODES:
            raise ValueError(
                f"The selected write_mode '{self.write_mode}' is invalid. The possible values are 'insert', 'upsert', 'do_nothing'."
            )

        self.command = "INSERT"
        if self.write_mode == "upsert":
            self.command = "UPSERT"

        self.suffix = ""
        if self.write_mode == "do_nothing":
            self.suffix = " ON CONFLICT DO NOTHING"

        # placeholders
        columns_ph = "%s," * len(self.value_types)
        rows_ph = f"(%s,{columns_ph[:-1]})," * self.batch_size
        self.placeholders = rows_ph[:-1]

        # create random generator
        # Not implemented as it needs a FR in pgworkload
        # self.rng = random.Random(int(self.seed) if self.seed else None)
        self.rng = random.Random(None)

        # create pool to pick the key from
        self.key_pool = deque(
            [self.__get_data(self.key_type, self.key_size)],
            maxlen=self.key_pool_size,
        )
        
        # AOST
        if self.aost:
            if self.aost == "fr":
                self.aost = "AS OF SYSTEM TIME follower_read_timestamp()"
            else:
                self.aost = f"AS OF SYSTEM TIME '{self.aost}'"
                
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
        rnd = random.random()
        if rnd < self.read_pct:
            return [self.read_kv, self.__think__] * self.cycle_size
        elif rnd < self.update_pct:
            return [self.update_kv, self.__think__] * self.cycle_size
        elif rnd < self.delete_pct:
            return [self.delete_kv, self.__think__] * self.cycle_size
        return [self.write_kv, self.__think__] * self.cycle_size

    def __think__(self, conn: psycopg.Connection):
        time.sleep(self.think_time)

    def __get_data(self, data_type: str, size: int):
        if data_type == "bytes":
            return self.rng.getrandbits(8 * size).to_bytes(size, "big")
        elif data_type == "uuid":
            return uuid.UUID(int=self.rng.getrandbits(128), version=4)
        elif data_type == "int":
            return self.rng.randint(0, 2**63 - 1)
        elif data_type == "string":
            return (
                self.rng.getrandbits(8 * size)
                .to_bytes(size, "big")
                .translate(self.tbl)
                .decode()
            )

    def read_kv(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {self.table_name} {self.aost} WHERE k = %s",
                (random.choice(self.key_pool),),
            ).fetchone()

    def update_kv(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {self.table_name} SET v = %s WHERE k = %s",
                (
                    self.__get_data(self.value_types[0], self.value_sizes[0]),
                    random.choice(self.key_pool),
                ),
            )

    def write_kv(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            args = []
            for _ in range(self.batch_size):
                k = self.__get_data(self.key_type, self.key_size)
                self.key_pool.append(k)
                args.append(k)
                args.extend(
                    [
                        self.__get_data(t, s)
                        for t, s in self.value_types_and_sizes.items()
                    ]
                )

            cur.execute(
                f"{self.command} INTO {self.table_name} VALUES {self.placeholders} {self.suffix}",
                args,
            )

    def delete_kv(self, conn: psycopg.Connection):
        # make sure to keep at least 1 item in the pool
        if len(self.key_pool) <= 1:
            return

        key = random.choice(self.key_pool)
        self.key_pool.remove(key)

        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {self.table_name} WHERE k = %s", (key,))
