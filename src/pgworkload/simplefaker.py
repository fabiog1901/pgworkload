import bisect
import datetime as dt
import itertools
import logging
import math
import numpy as np
import string
import uuid
import pandas as pd
import multiprocessing as mp
import os
import csv


class SimpleFaker:

    def __init__(self, seed: int = None, csv_max_rows: int = 1000000, compression: str = 'gzip'):
        self.csv_max_rows: int = csv_max_rows
        self.compression: str = compression
        self.rng: np.random.Generator = np.random.default_rng(seed=seed)

    class Costant:
        """Iterator that counts upward forever."""

        def __init__(self, value: str, null_pct: float, bitgenerator: np.random.PCG64):
            self.value: str = 'simplefaker' if value is None else value
            self.bitgen: np.random.PCG64 = np.random.PCG64(
            ) if bitgenerator is None else bitgenerator

            self.null_pct: float = 0.0 if null_pct is None else null_pct
            self.rng: np.random.Generator = np.random.Generator(self.bitgen)

        def __next__(self):
            if self.rng.random() < self.null_pct:
                return ''
            return self.value

    class Sequence:
        """Iterator that counts upward forever."""

        def __init__(self, start: int):
            self.start: int = 0 if start is None else start

        def __next__(self):
            start: int = self.start
            self.start += 1
            return start

    class UUIDv4:
        """Iterator thar yields a UUIDv4
        """

        def __init__(self, null_pct: float, bitgenerator: np.random.PCG64, array: int):
            self.bitgen: np.random.PCG64 = np.random.PCG64(
            ) if bitgenerator is None else bitgenerator

            self.array: int = 0 if array is None else array
            self.null_pct: float = 0.0 if null_pct is None else null_pct
            self.rng: np.random.Generator = np.random.Generator(self.bitgen)

        def __next__(self):
            if self.rng.random() < self.null_pct:
                return ''
            else:
                if not self.array:
                    return uuid.UUID(bytes=self.rng.bytes(16), version=4)
                else:
                    return 'ARRAY[%s]' % ','.join(f"'{x}'" for x in [uuid.UUID(bytes=self.rng.bytes(16), version=4) for _ in range(self.array)])

    class Timestamp:
        """Iterator that yields a Timestamp string
        """

        def __init__(self, start: str, end: str, format: str, null_pct: float, bitgenerator: np.random.PCG64, array: int):
            self.format: str = '%Y-%m-%d %H:%M:%S.%f' if format is None else format
            self._start: str = '2022-01-01' if start is None else start
            self._end: str = '2022-12-31' if end is None else end
            self.start: float = dt.datetime.fromisoformat(
                self._start).timestamp() * 1000000

            self.end: float = dt.datetime.fromisoformat(
                self._end).timestamp() * 1000000

            self.array: int = 0 if array is None else array
            self.null_pct: float = 0.0 if null_pct is None else null_pct
            self.bitgen: np.random.PCG64 = np.random.PCG64(
            ) if bitgenerator is None else bitgenerator

            self.rng: np.random.Generator = np.random.Generator(self.bitgen)

        def __next__(self):
            if self.rng.random() < self.null_pct:
                return ''
            else:
                if not self.array:
                    return dt.datetime.fromtimestamp(self.rng.integers(self.start, self.end)/1000000).strftime(self.format)
                else:
                    return 'ARRAY[%s]' % ','.join(f"'{x}'" for x in [dt.datetime.fromtimestamp(self.rng.integers(self.start, self.end)/1000000).strftime(self.format) for _ in range(self.array)])

    class Date(Timestamp):
        """Iterator that yields a Date string
        """

        def __init__(self, start: str, end: str, format: str, null_pct: float, bitgenerator: np.random.PCG64, array: int):
            self.format: str = '%Y-%m-%d' if format is None else format

            super().__init__(start=start, end=end, format=self.format,
                             null_pct=null_pct, bitgenerator=bitgenerator, array=array)

    class Time(Timestamp):
        """Iterator that yields a Time string
        """

        def __init__(self, start: str, end: str, micros: bool, null_pct: float, bitgenerator: np.random.PCG64, array: int):
            self.format: str = '%H:%M:%S' if not micros else '%H:%M:%S.%f'
            self._start: str = '07:30:00' if start is None else start
            self._end: str = '15:30:00' if end is None else end

            super().__init__(start='1970-01-01 ' + self._start,
                             end='1970-01-01 ' + self._end,
                             format=self.format, null_pct=null_pct, bitgenerator=bitgenerator, array=array)

    class String:
        """Iterator that yields a random string of ascii characters
        """

        def __init__(self, min: int, max: int, null_pct: float, bitgenerator: np.random.PCG64, array: int):
            self.min: int = 10 if min is None or min < 0 else min
            self.max: int = self.min + 20 if max is None or max < self.min else max
            self.letters: np.array = np.array(
                [char for char in string.ascii_letters])
            self.bitgen: np.random.PCG64 = np.random.PCG64(
            ) if bitgenerator is None else bitgenerator

            self.array: int = 0 if array is None else array
            self.null_pct: float = 0.0 if null_pct is None else null_pct
            self.rng: np.random.Generator = np.random.Generator(self.bitgen)

        def __next__(self):
            if self.rng.random() < self.null_pct:
                return ''
            else:
                if not self.array:
                    return ''.join(self.rng.choice(self.letters, size=(self.rng.integers(self.min, self.max))))
                else:
                    return 'ARRAY[%s]' % ','.join(f"'{x}'" for x in [''.join(self.rng.choice(self.letters, size=(self.rng.integers(self.min, self.max)))) for _ in range(self.array)])

    class Json(String):
        """Iterator that yields a simple json string
        """

        def __init__(self, min_num: int, max_num: int, null_pct: float, bitgenerator: np.random.PCG64):
            # 9 is the number of characters in the hardcoded string
            self.min = 10 if min_num is None else max(min_num-9, 1)
            self.max = 50 if max_num is None else max(max_num-9, 2)
            super().__init__(min=self.min, max=self.max,
                             null_pct=null_pct, bitgenerator=bitgenerator)

        def __next__(self):
            v = super().__next__()
            if not v:
                return ''
            return '{"k":"%s"}' % v

    class Integer:
        """Iterator that yields a random integer
        """

        def __init__(self, min: int, max: int, null_pct: float, bitgenerator: np.random.PCG64, array: int):
            self.min: int = 1000 if min is None else min
            self.max: int = 9999 if max is None else max
            self.bitgen: np.random.PCG64 = np.random.PCG64(
            ) if bitgenerator is None else bitgenerator

            self.array: int = 0 if array is None else array
            self.null_pct: float = 0.0 if null_pct is None else null_pct
            self.rng: np.random.Generator = np.random.Generator(self.bitgen)

        def __next__(self):
            if self.rng.random() < self.null_pct:
                return ''
            else:
                if not self.array:
                    return self.rng.integers(self.min, self.max)
                else:
                    return 'ARRAY[%s]' % ','.join(f"{x}" for x in [self.rng.integers(self.min, self.max) for _ in range(self.array)])

    class Bool(Integer):
        """Iterator that yields a random boolean (0, 1)
        """

        def __init__(self, null_pct: float, bitgenerator: np.random.PCG64, array: int):
            super().__init__(min=0, max=1, null_pct=null_pct,
                             bitgenerator=bitgenerator, array=array)

    class Float:
        """Iterator that yields a random float number
        """

        def __init__(self, max: int, round: int, null_pct: float, bitgenerator: np.random.PCG64, array: int):
            self.max: int = 1000 if max is None else max
            self.round: int = 2 if round is None else round
            self.bitgen: np.random.PCG64 = np.random.PCG64(
            ) if bitgenerator is None else bitgenerator

            self.array: int = 0 if array is None else array
            self.null_pct: float = 0.0 if null_pct is None else null_pct
            self.rng: np.random.Generator = np.random.Generator(self.bitgen)

        def __next__(self):
            if self.rng.random() < self.null_pct:
                return ''
            else:
                if not self.array:
                    return round(self.rng.random() * self.max, self.round)
                else:
                    return 'ARRAY[%s]' % ','.join(f"{x}" for x in [round(self.rng.random() * self.max, self.round) for _ in range(self.array)])

    class Bytes:
        """Iterator that yields a random byte array
        """

        def __init__(self, n: int, null_pct: float, bitgenerator: np.random.PCG64, array: int):
            self.n: int = 1 if n is None else n

            self.bitgen: np.random.PCG64 = np.random.PCG64(
            ) if bitgenerator is None else bitgenerator

            self.array: int = 0 if array is None else array
            self.null_pct: float = 0.0 if null_pct is None else null_pct
            self.rng: np.random.Generator = np.random.Generator(self.bitgen)

        def __next__(self):
            if self.rng.random() < self.null_pct:
                return ''
            else:
                if not self.array:
                    return self.rng.bytes(self.n)
                else:
                    return 'ARRAY[%s]' % ','.join(f"'{x}'" for x in [self.rng.bytes(self.n) for _ in range(self.array)])

    class Choice:
        """Iterator that yields 1 item from a list
        """

        def __init__(self, population: list, weights: list, cum_weights: list, null_pct: float, bitgenerator: np.random.PCG64, array: int):
            self.population: list = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri',
                                     'Sat', 'Sun'] if population is None else population
            self.weights: list = None if not weights else weights
            self.cum_weights: list = None if not cum_weights else cum_weights

            self.bitgen: np.random.PCG64 = np.random.PCG64(
            ) if bitgenerator is None else bitgenerator

            self.array: int = 0 if array is None else array
            self.null_pct: float = 0.0 if null_pct is None else null_pct
            self.rng: np.random.PCG64 = np.random.Generator(self.bitgen)

        def __next__(self):
            if self.rng.random() < self.null_pct:
                return ''
            else:
                if not self.array:
                    return self.__choices(self.population, weights=self.weights, cum_weights=self.cum_weights)[0]
                else:
                    return 'ARRAY[%s]' % ','.join(f"'{x}'" for x in [self.__choices(self.population, weights=self.weights, cum_weights=self.cum_weights)[0] for _ in range(self.array)])

        def __choices(self, population, weights=None, *, cum_weights=None, k=1):
            """Return a k sized list of population elements chosen with replacement.
            If the relative weights or cumulative weights are not specified,
            the selections are made with equal probability.
            """
            random = self.rng.random
            n = len(population)
            if cum_weights is None:
                if weights is None:
                    floor = math.floor
                    n += 0.0    # convert to float for a small speed improvement
                    return [population[floor(random() * n)] for i in itertools.repeat(None, k)]
                try:
                    cum_weights = list(itertools.accumulate(weights))
                except TypeError:
                    if not isinstance(weights, int):
                        raise
                    k = weights
                    raise TypeError(
                        f'The number of choices must be a keyword argument: {k=}'
                    ) from None
            elif weights is not None:
                raise TypeError(
                    'Cannot specify both weights and cumulative weights')
            if len(cum_weights) != n:
                raise ValueError(
                    'The number of weights does not match the population')
            total = cum_weights[-1] + 0.0   # convert to float
            if total <= 0.0:
                raise ValueError('Total of weights must be greater than zero')
            hi = n - 1
            return [population[bisect.bisect(cum_weights, random() * total, 0, hi)]
                    for i in itertools.repeat(None, k)]

    def __get_simplefaker_objects(self, type: str, args: dict, count: int, exec_threads: int):
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
        array: int = args.get('array', 0)
        null_pct: float = args.get('null_pct', 0.0)

        start = args.get('start')
        end = args.get('end')
        format = args.get('format')
        micros = args.get('micros')

        # integer/float
        min = args.get('min')
        max = args.get('max')
        n = args.get('n')
        round = args.get('round')

        # choice
        population = args.get('population')
        weights = args.get('weights')
        cum_weights = args.get('cum_weights')

        # costant
        value = args.get('value')

        # all types
        seed = args.get('seed', self.rng.integers(0, 1000000000))

        bitgens = [np.random.PCG64(x) for x in np.random.SeedSequence(
            seed).spawn(exec_threads)]

        type = type.lower()

        if type == 'integer':
            return [SimpleFaker.Integer(min=min, max=max, null_pct=null_pct, bitgenerator=bitgen, array=array) for bitgen in bitgens]
        elif type in ['float', 'decimal']:
            return [SimpleFaker.Float(max=max, round=round, null_pct=null_pct, bitgenerator=bitgen, array=array) for bitgen in bitgens]
        elif type == 'bool':
            return [SimpleFaker.Bool(null_pct, bitgen, array) for bitgen in bitgens]
        elif type == 'string':
            return [SimpleFaker.String(min, max, null_pct, bitgen, array) for bitgen in bitgens]
        elif type in ['json', 'jsonb']:
            return [SimpleFaker.Json(min, max, null_pct, bitgen) for bitgen in bitgens]
        elif type == 'bytes':
            return [SimpleFaker.Bytes(n, null_pct, bitgen, array) for bitgen in bitgens]
        elif type == 'choice':
            return [SimpleFaker.Choice(population, weights, cum_weights, null_pct, bitgen, array) for bitgen in bitgens]
        elif type in ['uuidv4', 'uuid']:
            return [SimpleFaker.UUIDv4(null_pct, bitgen, array) for bitgen in bitgens]
        elif type == 'timestamp':
            return [SimpleFaker.Timestamp(start, end, format, null_pct, bitgen, array) for bitgen in bitgens]
        elif type == 'time':
            return [SimpleFaker.Time(start, end, micros, null_pct, bitgen, array) for bitgen in bitgens]
        elif type == 'date':
            return [SimpleFaker.Date(start, end, format, null_pct, bitgen, array) for bitgen in bitgens]
        elif type == 'costant':
            return [SimpleFaker.Costant(value, null_pct, bitgen) for bitgen in bitgens]
        elif type == 'sequence':
            div = int(count/exec_threads)
            return [SimpleFaker.Sequence(div * x + start) for x in range(exec_threads)]
        else:
            raise ValueError(
                f"SimpleFaker type not implemented or recognized: '{type}'")

    def worker(self, generators: tuple, iterations: int, basename: str, col_names: list, sort_by: list, separator: str):
        logging.debug("SimpleFaker worker created")
        if iterations > self.csv_max_rows:
            count = int(iterations/self.csv_max_rows)
            rem = iterations % self.csv_max_rows
            iterations = self.csv_max_rows
        else:
            count = 1
            rem = 0

        if self.compression == 'gzip':
            suffix = '.csv.gz'
        elif self.compression == 'zip':
            suffix = '.csv.zip'
        elif self.compression == None:
            suffix = '.csv'
        else:
            suffix = '.csv'

        for x in range(count):
            pd.DataFrame(
                [row for row in [[next(x) for x in generators]
                                 for _ in range(iterations)]],
                columns=col_names)\
                .sort_values(by=sort_by)\
                .to_csv(basename + '_' + str(x) + suffix, quoting=csv.QUOTE_NONE, sep=separator, header=False, index=False, compression=self.compression)

        # remaining rows, if any
        if rem > 0:
            pd.DataFrame(
                [row for row in [[next(x) for x in generators]
                                 for _ in range(rem)]],
                columns=col_names)\
                .sort_values(by=sort_by)\
                .to_csv(basename + '_' + str(count) + suffix, sep=separator, header=False, index=False, compression=self.compression)

    def __division_with_modulo(self, total: int, divider: int):
        rows_to_process = int(total/divider)
        rows_left_over = total % divider

        if rows_left_over == 0:
            return [rows_to_process] * divider
        else:
            l = [rows_to_process] * (divider-1)
            l.append(rows_to_process + rows_left_over)
            return l

    def __write_csvs(self, obj, basename, col_names, sort_by, exec_threads, delimiter):
        logging.debug('Writing CSV files...')

        # create a zip object so that generators are paired together
        z = zip(*[x for x in obj['tables'].values()])

        rows_chunk = self.__division_with_modulo(obj['count'], exec_threads)
        procs = []
        for i, rows in enumerate(rows_chunk):
            output_file = basename + '_' + str(i)

            p = mp.Process(target=self.worker, args=(
                next(z), rows, output_file, col_names, sort_by, delimiter))
            p.start()
            procs.append(p)

        # wait for all workers to exit
        for p in procs:
            p.join()

    def generate(self, load: dict, exec_threads: int, csv_dir: str, delimiter: str):

        for table_name, table_details in load.items():
            csv_file_basename = os.path.join(csv_dir, table_name)

            logging.info("Generating dataset for table '%s'" % table_name)

            for item in table_details:
                col_names = list(item['tables'].keys())
                sort_by = item.get('sort-by', [])
                for col, col_details in item['tables'].items():
                    # get the list of simplefaker objects with different seeds
                    item['tables'][col] = self.__get_simplefaker_objects(
                        col_details['type'], col_details['args'], item['count'], exec_threads)

                self.__write_csvs(item, csv_file_basename + '.' +
                                  str(table_details.index(item)), col_names, sort_by, exec_threads, delimiter)
