import psycopg
import random
import time
import uuid


class Kv:
    def __init__(self, args: dict):
        # default think_time is 10ms
        self.think_time: float = float(args.get("think_time", 10) / 1000)

        # default batch_size is 1
        self.batch_size: int = int(args.get("batch_size", 1))

        self.key_size: int = int(args.get("key_size", 100))
        self.payload_size: int = int(args.get("payload_size", 100))

        self.mode: str = args.get("mode", "bytes")

        if self.mode not in ["bytes", "uuid", "int"]:
            raise ValueError(
                f"The selected mode '{self.mode}' is invalid. The possible values are 'bytes', 'uuid', 'int'."
            )

    def run(self):
        return [self.insert_bytes, self.think] * 100

    def think(self, conn: psycopg.Connection):
        time.sleep(self.think_time)

    def __get_key(self):
        if self.mode == "bytes":
            return random.getrandbits(8 * self.key_size).to_bytes(self.key_size, "big")
        elif self.mode == "uuid":
            return uuid.uuid4()
        elif self.mode == "int":
            return random.randint(0, 1e18)

    def insert_bytes(self, conn: psycopg.Connection):
        placeholders = "(%s, %s)," * self.batch_size

        with conn.cursor() as cur:
            stmt = f"insert into kv_{self.mode} (k, v)  values {placeholders[:-1]}"

            args = []
            for _ in range(self.batch_size):
                args.extend(
                    [
                        self.__get_key(),
                        random.getrandbits(8 * self.payload_size).to_bytes(
                            self.payload_size, "big"
                        ),
                    ]
                )

            cur.execute(stmt, tuple(args))
