import csv
import datetime as dt
import logging
import multiprocessing as mp
import os
import pandas as pd
import uuid
import random

logger = logging.getLogger("pgworkload")


class SimpleFaker:
    """Pseudo-random data generator based on
    the random.Random class.
    """

    def __init__(self, seed: float = None, csv_max_rows: int = 100000):
        self.csv_max_rows = csv_max_rows
        self.rng: random.Random = random.Random(seed)

    class Abc:
        def __init__(self, seed: float, null_pct: float = 0, array: int = 0):
            self.array = array
            self.null_pct = null_pct
            self.rng: random.Random = random.Random(seed)

    class Constant(Abc):
        """Iterator always yields the same value."""

        def __init__(
            self, value: str = "simplefaker", seed: float = 0, null_pct: float = 0
        ):
            super().__init__(seed, null_pct, 0)
            self.value = value

        def __next__(self):
            if self.rng.random() < self.null_pct:
                return ""
            return self.value

    class Sequence:
        """Iterator that counts upward forever."""

        def __init__(self, start: int = 0):
            self.start = start

        def __next__(self):
            start: int = self.start
            self.start += 1
            return start

    class UUIDv4(Abc):
        """Iterator thar yields a UUIDv4"""

        def __init__(self, seed: float, null_pct: float, array: int):
            super().__init__(seed, null_pct, array)

        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                if not self.array:
                    return uuid.UUID(int=self.rng.getrandbits(128), version=4)
                else:
                    return "{%s}" % ",".join(
                        [
                            str(uuid.UUID(int=self.rng.getrandbits(128), version=4))
                            for _ in range(self.array)
                        ]
                    )

    class Timestamp(Abc):
        """Iterator that yields a Timestamp string"""

        def __init__(
            self,
            start: str = "2000-01-01",
            end: str = "2024-12-31",
            format: str = "%Y-%m-%d %H:%M:%S.%f",
            seed: float = 0,
            null_pct: float = 0,
            array: int = 0,
        ):
            super().__init__(seed, null_pct, array)
            self.format = format
            self.start = int(dt.datetime.fromisoformat(start).timestamp()) * 1000000
            self.end = int(dt.datetime.fromisoformat(end).timestamp()) * 1000000

        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                if not self.array:
                    return dt.datetime.fromtimestamp(
                        self.rng.randint(self.start, self.end) / 1000000
                    ).strftime(self.format)
                else:
                    return "{%s}" % ",".join(
                        [
                            dt.datetime.fromtimestamp(
                                self.rng.randint(self.start, self.end) / 1000000
                            ).strftime(self.format)
                            for _ in range(self.array)
                        ]
                    )

    class Date(Timestamp):
        """Iterator that yields a Date string"""

        def __init__(
            self,
            start: str = "2000-01-01",
            end: str = "2024-12-31",
            format: str = "%Y-%m-%d",
            seed: float = 0,
            null_pct: float = 0,
            array: int = 0,
        ):
            super().__init__(
                start=start,
                end=end,
                format=format,
                null_pct=null_pct,
                seed=seed,
                array=array,
            )

    class Time(Timestamp):
        """Iterator that yields a Time string"""

        def __init__(
            self,
            start: str = "07:30:00",
            end: str = "22:30:00",
            micros: bool = False,
            seed: float = 0,
            null_pct: float = 0,
            array: int = 0,
        ):
            super().__init__(
                start="1970-01-01 " + start,
                end="1970-01-01 " + end,
                format="%H:%M:%S" if not micros else "%H:%M:%S.%f",
                null_pct=null_pct,
                seed=seed,
                array=array,
            )

    class String(Abc):
        """Iterator that yields a truly random string of ascii characters"""

        def __init__(
            self,
            min: int = 10,
            max: int = 50,
            prefix: str = "",
            seed: float = 0.0,
            null_pct: float = 0.0,
            array: int = 0,
        ):
            super().__init__(seed, null_pct, array)
            self.min = min
            self.max = max
            self.prefix = prefix

            assert min >= 0
            assert min <= max

            # make translation table from 0..255 to 97..122
            self.tbl = bytes.maketrans(
                bytearray(range(256)),
                bytearray(
                    [ord(b"a") + b % 26 for b in range(113)]
                    + [ord(b"0") + b % 10 for b in range(30)]
                    + [ord(b"A") + b % 26 for b in range(113)]
                ),
            )

        # generate random bytes and translate them to ascii
        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                size = self.rng.randint(self.min, self.max)
                if not self.array:
                    return (
                        self.prefix
                        + self.rng.getrandbits(8 * size)
                        .to_bytes(size, "big")
                        .translate(self.tbl)
                        .decode()
                    )
                else:
                    return "{%s}" % ",".join(
                        f"{self.prefix}{x}"
                        for x in [
                            self.rng.getrandbits(size * 8)
                            .to_bytes(size, "big")
                            .translate(self.tbl)
                            .decode()
                            for _ in range(self.array)
                        ]
                    )

    class Json(String):
        """Iterator that yields a simple json string"""

        def __init__(
            self,
            min_num: int = 10,
            max_num: int = 50,
            seed: float = 0,
            null_pct: float = 0,
        ):
            # 9 is the number of characters in the hardcoded string
            self.min = max(min_num - 9, 1)
            self.max = max(max_num - 9, 2)
            super().__init__(
                min=self.min, max=self.max, null_pct=null_pct, seed=seed, array=0
            )

        def __next__(self):
            v = super().__next__()
            if not v:
                return ""
            return '{"k":"%s"}' % v

    class Integer(Abc):
        """Iterator that yields a random integer"""

        def __init__(
            self,
            min_num: int = 1,
            max_num: int = 1000000000,
            seed: float = 0,
            null_pct: float = 0,
            array: int = 0,
        ):
            super().__init__(seed, null_pct, array)
            self.min_num = min_num
            self.max_num = max_num

        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                if not self.array:
                    return self.rng.randint(self.min_num, self.max_num)
                else:
                    return "{%s}" % ",".join(
                        [
                            str(self.rng.randint(self.min_num, self.max_num))
                            for _ in range(self.array)
                        ]
                    )

    class Bit(Abc):
        """Iterator that yields random bits"""

        def __init__(
            self,
            size: int = 10,
            seed: float = 0,
            null_pct: float = 0,
            array: int = 0,
        ):
            super().__init__(seed, null_pct, array)
            self.size = size

        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                if not self.array:
                    return "".join(
                        [str(int(self.rng.random() > 0.5)) for _ in range(self.size)]
                    )
                else:
                    return "{%s}" % ",".join(
                        [
                            "".join(
                                [
                                    str(int(self.rng.random() > 0.5))
                                    for _ in range(self.size)
                                ]
                            )
                            for _ in range(self.array)
                        ]
                    )

    class Bool(Abc):
        """Iterator that yields a random boolean (0, 1)"""

        def __init__(self, seed: float, null_pct: float, array: int):
            super().__init__(seed, null_pct, array)

        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                if not self.array:
                    return int(self.rng.random() > 0.5)
                else:
                    return "{%s}" % ",".join(
                        [str(int(self.rng.random() > 0.5)) for _ in range(self.array)]
                    )

    class Float(Abc):
        """Iterator that yields a random float number"""

        def __init__(
            self,
            min: int = 0,
            max: int = 1000,
            round: int = 2,
            seed: float = 0,
            null_pct: float = 0,
            array: int = 0,
        ):
            super().__init__(seed, null_pct, array)
            self.min = min
            self.max = max - 1  # max value must not be inclusive
            self.round = round

        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                if not self.array:
                    return round(self.rng.uniform(self.min, self.max), self.round)
                else:
                    return "{%s}" % ",".join(
                        [
                            str(round(self.rng.uniform(self.min, self.max), self.round))
                            for _ in range(self.array)
                        ]
                    )

    class Bytes(Abc):
        """Iterator that yields a random byte array"""

        def __init__(
            self,
            size: int = 20,
            seed: float = 0,
            null_pct: float = 0,
            array: int = 0,
        ):
            self.size = size
            super().__init__(seed, null_pct, array)

            # make translation table from 0..255 to hex chars 'abcdef0123456789'
            self.hex_tbl = bytes.maketrans(
                bytearray(range(256)),
                bytearray(
                    [ord(b"a") + b % 6 for b in range(160)]
                    + [ord(b"0") + b % 10 for b in range(96)]
                ),
            )

        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                if not self.array:
                    return "\\x" + (
                        self.rng.getrandbits(8 * self.size)
                        .to_bytes(self.size, "big")
                        .translate(self.hex_tbl)
                        .decode()
                    )

                else:
                    return "{%s}" % ",".join(
                        f'"\\\\x{x}"'
                        for x in [
                            self.rng.getrandbits(self.size * 8)
                            .to_bytes(self.size, "big")
                            .translate(self.hex_tbl)
                            .decode()
                            for _ in range(self.array)
                        ]
                    )

    class Choice(Abc):
        """Iterator that yields 1 item from a list"""

        def __init__(
            self,
            population: list = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
            weights: list = None,
            cum_weights: list = None,
            seed: float = 0,
            null_pct: float = 0,
            array: int = 0,
        ):
            super().__init__(seed, null_pct, array)
            self.population = population
            self.weights = weights
            self.cum_weights = cum_weights

        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                if not self.array:
                    return self.rng.choices(
                        self.population,
                        weights=self.weights,
                        cum_weights=self.cum_weights,
                    )[0]
                else:
                    return "{%s}" % ",".join(
                        [
                            self.rng.choices(
                                self.population,
                                weights=self.weights,
                                cum_weights=self.cum_weights,
                            )[0]
                            for _ in range(self.array)
                        ]
                    )

    def division_with_modulo(self, total: int, divider: int):
        """Split a number into chunks.
        Eg: total=10, divider=3 returns [3,3,4]

        Args:
            total (int): The number to divide
            divider (int): the count of chunks

        Returns:
            (list): the list of the individual chunks
        """
        rows_to_process = int(total / divider)
        rows_left_over = total % divider

        if rows_left_over == 0:
            return [rows_to_process] * divider
        else:
            l = [rows_to_process] * (divider - 1)
            l.append(rows_to_process + rows_left_over)
            return l

    """
    the seed in the yaml file `1` is used to create a rng used to 
    generate the seed number for each SimpleFaker class
    
    >>> rng = random.Random(1)
    >>> seed1 = rng.random()
    >>> seed2 = rng.random()
    >>> 
    >>> 
    >>> rng1 = random.Random(seed1)
    >>> rng1.randint(0, 1000000)
    948735
    >>> rng1.randint(0, 1000000)
    614390
    >>> 
    >>> rng2 = random.Random(seed2)
    >>> rng2.randint(0, 1000000)
    140545
    >>> rng2.randint(0, 1000000)
    157597
    """

    def generate(
        self,
        load: dict,
        exec_threads: int,
        csv_dir: str,
        delimiter: str,
        compression: str,
    ):
        """Generate the CSV datasets

        Args:
            load (dict): the data generation definition
            exec_threads (int): count of processes for parallel execution
            csv_dir (str): destination directory for the CSV files
            delimiter (str): field delimiter
            compression (str): the compression format (gzip, zip, None..)
        """

        for table_name, table_details in load.items():
            csv_file_basename = os.path.join(csv_dir, table_name)

            logger.info(f"Generating dataset for table '{table_name}'")

            for item in table_details:
                col_names = list(item["columns"].keys())
                sort_by = item.get("sort-by", [])
                for col, col_details in item["columns"].items():
                    # get the list of simplefaker objects with different seeds
                    item["columns"][col] = self.__get_simplefaker_objects(
                        col_details["type"],
                        col_details["args"],
                        item["count"],
                        exec_threads,
                    )

                # create a zip object so that generators are paired together
                z = zip(*[x for x in item["columns"].values()])

                rows_chunk = self.division_with_modulo(item["count"], exec_threads)
                procs = []
                for i, rows in enumerate(rows_chunk):
                    output_file = (
                        csv_file_basename
                        + "."
                        + str(table_details.index(item))
                        + "_"
                        + str(i)
                    )

                    p = mp.Process(
                        target=self.worker,
                        daemon=True,
                        args=(
                            next(z),
                            rows,
                            output_file,
                            col_names,
                            sort_by,
                            delimiter,
                            compression,
                        ),
                    )
                    p.start()
                    procs.append(p)

                # wait for all workers to exit
                for p in procs:
                    p.join()

    def __get_simplefaker_objects(
        self, type: str, args: dict, count: int, exec_threads: int
    ):
        """Returns a list of SimpleFaker objects based on the number of execution threads.
        Each SimpleFaker object in the list has its own seed number

        Args:
            type (str): the name of object to create
            args (dict): args required to create the SimpleFaker object
            count (int): count of rows to generate (for SimpleFaker.Sequence obj)
            exec_threads (int): count of parallel processes/threads used for data generation

        Returns:
            list: a <exec_threads> long list of SimpleFaker objects of type <type>
        """

        # Extract all possible args for all possible types to avoid repetition
        # date, time, timestamp, string.
        array: int = args.get("array", 0)
        null_pct: float = args.get("null_pct", 0.0)

        start: str = args.get("start", "")
        end: str = args.get("end", "")
        prefix: str = args.get("prefix", "")
        format: str = args.get("format", "")
        micros: bool = args.get("micros", False)

        # integer/float
        min: int = args.get("min", 0)
        max: int = args.get("max", 10)
        round: int = args.get("round", 2)

        # choice
        population: list = args.get("population", ["a", "b", "c"])
        weights: list = args.get("weights", None)
        cum_weights: list = args.get("cum_weights", None)

        # constant
        value: str = args.get("value", "")

        # bit
        size: int = args.get("size", 10)

        # all types
        seed: float = args.get("seed", self.rng.random())

        # create a list of pseudo random seeds
        r = random.Random(seed)
        seeds = [r.random() for _ in range(exec_threads)]

        type = type.lower()

        if type == "integer":
            return [
                SimpleFaker.Integer(min, max, seed, null_pct, array) for seed in seeds
            ]
        elif type in ["float", "decimal"]:
            return [
                SimpleFaker.Float(min, max, round, seed, null_pct, array)
                for seed in seeds
            ]
        elif type == "bool":
            return [SimpleFaker.Bool(seed, null_pct, array) for seed in seeds]
        elif type == "string":
            return [
                SimpleFaker.String(min, max, prefix, seed, null_pct, array)
                for seed in seeds
            ]
        elif type in ["json", "jsonb"]:
            return [SimpleFaker.Json(min, max, seed, null_pct) for seed in seeds]
        elif type == "bytes":
            return [SimpleFaker.Bytes(size, seed, null_pct, array) for seed in seeds]
        elif type == "choice":
            return [
                SimpleFaker.Choice(
                    population, weights, cum_weights, seed, null_pct, array
                )
                for seed in seeds
            ]
        elif type in ["uuidv4", "uuid"]:
            return [SimpleFaker.UUIDv4(seed, null_pct, array) for seed in seeds]
        elif type == "timestamp":
            return [
                SimpleFaker.Timestamp(start, end, format, seed, null_pct, array)
                for seed in seeds
            ]
        elif type == "time":
            return [
                SimpleFaker.Time(start, end, micros, seed, null_pct, array)
                for seed in seeds
            ]
        elif type == "date":
            return [
                SimpleFaker.Date(start, end, format, seed, null_pct, array)
                for seed in seeds
            ]
        elif type == "constant":
            return [SimpleFaker.Constant(value, seed, null_pct) for seed in seeds]
        elif type == "sequence":
            div = int(count / exec_threads)
            return [
                SimpleFaker.Sequence(div * x + int(start)) for x in range(exec_threads)
            ]
        elif type == "bit":
            div = int(count / exec_threads)
            return [
                SimpleFaker.Bit(size, seed, null_pct, array)
                for x in range(exec_threads)
            ]
        else:
            raise ValueError(
                f"SimpleFaker type not implemented or recognized: '{type}'"
            )

    def worker(
        self,
        generators: tuple,
        iterations: int,
        basename: str,
        col_names: list,
        sort_by: list,
        separator: str,
        compression: str,
    ):
        """Process worker function to generate the data in a multiprocessing env

        Args:
            generators (tuple): the SimpleFaker data gen objects
            iterations (int): count of rows to generate
            basename (str): the basename of the output csv file
            col_names (list): the csv column names, used for sorting
            sort_by (list): the column to sort by
            separator (str): the field delimiter in the CSV file
            compression (str): the compression format (gzip, zip, None..)
        """
        logger.debug("SimpleFaker worker created")
        if iterations > self.csv_max_rows:
            count = int(iterations / self.csv_max_rows)
            rem = iterations % self.csv_max_rows
            iterations = self.csv_max_rows
        else:
            count = 1
            rem = 0

        if compression == "gzip":
            suffix = ".csv.gz"
        elif compression == "zip":
            suffix = ".csv.zip"
        else:
            suffix = ".csv"

        for x in range(count):
            try:
                pd.DataFrame(
                    [
                        row
                        for row in [
                            [next(x) for x in generators] for _ in range(iterations)
                        ]
                    ],
                    columns=col_names,
                ).sort_values(by=sort_by).to_csv(
                    basename + "_" + str(x) + suffix,
                    quoting=csv.QUOTE_MINIMAL,
                    sep=separator,
                    header=False,
                    index=False,
                    compression=compression,
                )
            except csv.Error as e:
                logger.error(e)
                if e.args[0] == "need to escape, but no escapechar set":
                    logger.error(
                        f"You cannot use the selected delimiter '{separator}'. Consider using another char or the the tab key."
                    )

            logger.debug(f"Saved file '{basename + '_' + str(x) + suffix}'")

        # remaining rows, if any
        if rem > 0:
            pd.DataFrame(
                [row for row in [[next(x) for x in generators] for _ in range(rem)]],
                columns=col_names,
            ).sort_values(by=sort_by).to_csv(
                basename + "_" + str(count) + suffix,
                quoting=csv.QUOTE_MINIMAL,
                sep=separator,
                header=False,
                index=False,
                compression=compression,
            )

            logger.debug(f"Saved file '{basename + '_' + str(x) + suffix}'")
