import csv
import datetime as dt
import logging
import multiprocessing as mp
import os
import pandas as pd
import uuid
import random

logger = logging.getLogger(__name__)


class SimpleFaker:
    """Pseudo-random data generator based on
    the random.Random class.
    """

    def __init__(self, seed: float = None, csv_max_rows: int = 100000):
        self.csv_max_rows: int = csv_max_rows
        self.rng: random.Random = random.Random(seed)

    class Abc:
        def __init__(self, seed: float, null_pct: float, array: int):
            self.array: int = 0 if array is None else array
            self.null_pct: float = 0.0 if null_pct is None else null_pct
            self.rng: random.Random = random.Random(seed)

    class Constant(Abc):
        """Iterator always yields the same value."""

        def __init__(self, value: str, seed: float, null_pct: float):
            super().__init__(seed, null_pct, 0)
            self.value: str = "simplefaker" if value is None else value

        def __next__(self):
            if self.rng.random() < self.null_pct:
                return ""
            return self.value

    class Sequence:
        """Iterator that counts upward forever."""

        def __init__(self, start: int):
            self.start: int = 0 if start is None else start

        def __next__(self):
            start: int = self.start
            self.start += 1
            return start

    class UUIDv4(Abc):
        """Iterator thar yields a UUIDv4"""

        def __init__(self, seed: int, null_pct: float, array: int):
            super().__init__(seed, null_pct, array)

        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                if not self.array:
                    return uuid.UUID(int=self.rng.getrandbits(128), version=4)
                else:
                    return "ARRAY[%s]" % ",".join(
                        f"'{x}'"
                        for x in [
                            uuid.UUID(int=self.rng.getrandbits(128), version=4)
                            for _ in range(self.array)
                        ]
                    )

    class Timestamp(Abc):
        """Iterator that yields a Timestamp string"""

        def __init__(
            self,
            start: str,
            end: str,
            format: str,
            seed: float,
            null_pct: float,
            array: int,
        ):
            super().__init__(seed, null_pct, array)
            self.format: str = "%Y-%m-%d %H:%M:%S.%f" if format is None else format
            self._start: str = "2022-01-01" if start is None else start
            self._end: str = "2022-12-31" if end is None else end
            self.start: float = (
                dt.datetime.fromisoformat(self._start).timestamp() * 1000000
            )

            self.end: float = dt.datetime.fromisoformat(self._end).timestamp() * 1000000

        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                if not self.array:
                    return dt.datetime.fromtimestamp(
                        self.rng.randint(self.start, self.end) / 1000000
                    ).strftime(self.format)
                else:
                    return "ARRAY[%s]" % ",".join(
                        f"'{x}'"
                        for x in [
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
            start: str,
            end: str,
            format: str,
            seed: float,
            null_pct: float,
            array: int,
        ):
            self.format: str = "%Y-%m-%d" if format is None else format

            super().__init__(
                start=start,
                end=end,
                format=self.format,
                null_pct=null_pct,
                seed=seed,
                array=array,
            )

    class Time(Timestamp):
        """Iterator that yields a Time string"""

        def __init__(
            self,
            start: str,
            end: str,
            micros: bool,
            seed: float,
            null_pct: int,
            array: int,
        ):
            self.format: str = "%H:%M:%S" if not micros else "%H:%M:%S.%f"
            self._start: str = "07:30:00" if start is None else start
            self._end: str = "15:30:00" if end is None else end

            super().__init__(
                start="1970-01-01 " + self._start,
                end="1970-01-01 " + self._end,
                format=self.format,
                null_pct=null_pct,
                seed=seed,
                array=array,
            )

    class String(Abc):
        """Iterator that yields a truly random string of ascii characters"""

        def __init__(
            self, min: int, max: int, seed: float, null_pct: float, array: int
        ):
            super().__init__(seed, null_pct, array)
            self.min: int = 10 if min is None or min < 0 else min
            self.max: int = self.min + 20 if max is None or max < self.min else max

            # make translation table from 0..255 to 97..122
            self.tbl = bytes.maketrans(
                bytearray(range(256)),
                bytearray(
                    [ord(b"a") + b % 26 for b in range(113)]
                    + [ord(b"0") + b % 10 for b in range(30)]
                    + [ord(b"A") + b % 26 for b in range(113)]
                ),
            )

        # generate random bytes and translate them to lowercase ascii
        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                size = self.rng.randint(self.min, self.max)
                if not self.array:
                    return (
                        self.rng.getrandbits(8 * size)
                        .to_bytes(size, "big")
                        .translate(self.tbl)
                        .decode()
                    )
                else:
                    return "ARRAY[%s]" % ",".join(
                        f"'{x}'"
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

        def __init__(self, min_num: int, max_num: int, seed: float, null_pct: float):
            # 9 is the number of characters in the hardcoded string
            self.min = 10 if min_num is None else max(min_num - 9, 1)
            self.max = 50 if max_num is None else max(max_num - 9, 2)
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
            self, min_num: int, max_num: int, seed: float, null_pct: float, array: int
        ):
            super().__init__(seed, null_pct, array)
            self.min_num: int = 1000 if min_num is None else min_num
            self.max_num: int = 9999 if max_num is None else max_num

        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                if not self.array:
                    return self.rng.randint(self.min_num, self.max_num)
                else:
                    return "ARRAY[%s]" % ",".join(
                        f"{x}"
                        for x in [
                            self.rng.randint(self.min_num, self.max_num)
                            for _ in range(self.array)
                        ]
                    )

    class Bool(Integer):
        """Iterator that yields a random boolean (0, 1)"""

        def __init__(self, seed: float, null_pct: float, array: int):
            super().__init__(
                min_num=0, max_num=1, null_pct=null_pct, seed=seed, array=array
            )

    class Float(Abc):
        """Iterator that yields a random float number"""

        def __init__(
            self, max: int, round: int, seed: float, null_pct: float, array: int
        ):
            super().__init__(seed, null_pct, array)
            self.max: int = 1000 if max is None else max
            self.round: int = 2 if round is None else round

        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                if not self.array:
                    return round(self.rng.random() * self.max, self.round)
                else:
                    return "ARRAY[%s]" % ",".join(
                        f"{x}"
                        for x in [
                            round(self.rng.random() * self.max, self.round)
                            for _ in range(self.array)
                        ]
                    )

    class Bytes(Abc):
        """Iterator that yields a random byte array"""

        def __init__(self, n: int, seed: float, null_pct: float, array: int):
            super().__init__(seed, null_pct, array)
            self.n: int = 1 if n is None else n

        def __next__(self):
            if self.null_pct and self.rng.random() < self.null_pct:
                return ""
            else:
                if not self.array:
                    return self.rng.getrandbits(self.n * 8).to_bytes(self.n, "little")
                else:
                    return "ARRAY[%s]" % ",".join(
                        f"'{x}'"
                        for x in [
                            self.rng.getrandbits(self.n * 8).to_bytes(self.n, "little")
                            for _ in range(self.array)
                        ]
                    )

    class Choice(Abc):
        """Iterator that yields 1 item from a list"""

        def __init__(
            self,
            population: list,
            weights: list,
            cum_weights: list,
            seed: float,
            null_pct: float,
            array: int,
        ):
            super().__init__(seed, null_pct, array)
            self.population: list = (
                ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                if population is None
                else population
            )
            self.weights: list = None if not weights else weights
            self.cum_weights: list = None if not cum_weights else cum_weights

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
                    return "ARRAY[%s]" % ",".join(
                        f"'{x}'"
                        for x in [
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

        start = args.get("start")
        end = args.get("end")
        format = args.get("format")
        micros = args.get("micros")

        # integer/float
        min = args.get("min")
        max = args.get("max")
        n = args.get("n")
        round = args.get("round")

        # choice
        population = args.get("population")
        weights = args.get("weights")
        cum_weights = args.get("cum_weights")

        # constant
        value = args.get("value")

        # all types
        seed = args.get("seed", self.rng.random())

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
                SimpleFaker.Float(max, round, seed, null_pct, array) for seed in seeds
            ]
        elif type == "bool":
            return [SimpleFaker.Bool(seed, null_pct, array) for seed in seeds]
        elif type == "string":
            return [
                SimpleFaker.String(min, max, seed, null_pct, array) for seed in seeds
            ]
        elif type in ["json", "jsonb"]:
            return [SimpleFaker.Json(min, max, seed, null_pct) for seed in seeds]
        elif type == "bytes":
            return [SimpleFaker.Bytes(n, seed, null_pct, array) for seed in seeds]
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
            return [SimpleFaker.Sequence(div * x + start) for x in range(exec_threads)]
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
                quoting=csv.QUOTE_NONE,
                sep=separator,
                header=False,
                index=False,
                compression=compression,
            )
            logger.debug(f"Saved file '{basename + '_' + str(x) + suffix}'")

        # remaining rows, if any
        if rem > 0:
            pd.DataFrame(
                [row for row in [[next(x) for x in generators] for _ in range(rem)]],
                columns=col_names,
            ).sort_values(by=sort_by).to_csv(
                basename + "_" + str(count) + suffix,
                sep=separator,
                header=False,
                index=False,
                compression=compression,
            )

            logger.debug(f"Saved file '{basename + '_' + str(x) + suffix}'")
