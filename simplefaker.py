from bisect import bisect as _bisect
import datetime as dt
import itertools
import math
import numpy as np
import string
import uuid


class SimpleFaker:

    class Costant:
        """Iterator that counts upward forever."""

        def __init__(self, value):
            self.seed = 0
            self.value = value

        def __next__(self):
            return self.value

        def get_copy(self, bitgenerator: np.random.BitGenerator):
            return self

    class Sequence:
        """Iterator that counts upward forever."""

        def __init__(self, start=0):
            self.start = start

        def __next__(self):
            start = self.start
            self.start += 1
            return start

        def get_copy(self, start):
            return SimpleFaker.Sequence(start)

    class UUIDv4:
        def __init__(self, bitgenerator: np.random.PCG64 = None, seed: int = 0):
            self.seed = seed
            if bitgenerator is None:
                self.bitgen = np.random.PCG64(seed)
            else:
                self.bitgen = bitgenerator
            self.rng = np.random.Generator(self.bitgen)

        def __next__(self):
            return uuid.UUID(bytes=self.rng.bytes(16), version=4)

        def get_copy(self, bitgenerator: np.random.BitGenerator):
            return SimpleFaker.UUIDv4(bitgenerator=bitgenerator)

    class Timestamp:
        def __init__(self, start_ts, end_ts, format: str = '%Y-%m-%d %H:%M:%S.%f', bitgenerator: np.random.PCG64 = None, seed: int = 0):
            self.format = format
            self.start_iso = start_ts
            self.end_iso = end_ts
            self.start = dt.datetime.fromisoformat(
                start_ts).timestamp() * 1000000
            self.end = dt.datetime.fromisoformat(end_ts).timestamp() * 1000000

            self.seed = seed
            if bitgenerator is None:
                self.bitgen = np.random.PCG64(seed)
            else:
                self.bitgen = bitgenerator
            self.rng = np.random.Generator(self.bitgen)

        def __next__(self):
            return dt.datetime.fromtimestamp(self.rng.integers(self.start, self.end)/1000000).strftime(self.format)

        def get_copy(self, bitgenerator: np.random.BitGenerator):
            return SimpleFaker.Timestamp(self.start_iso, self.end_iso, self.format, bitgenerator=bitgenerator)

    class Date(Timestamp):
        def __init__(self, start, end, format: str = '%Y-%m-%d', bitgenerator: np.random.PCG64 = None, seed: int = 0):
            super().__init__(start_ts=start, end_ts=end, format=format, seed=seed)

        def __next__(self):
            return next(self)

    class Time(Timestamp):
        def __init__(self, start, end, show_micros: bool = False, bitgenerator: np.random.PCG64 = None, seed: int = 0):
            format = '%H:%M:%S.%f' if show_micros else '%H:%M:%S'
            super().__init__(start_ts='1970-01-01 ' + start,
                             end_ts='1970-01-01 ' + end, format=format, seed=seed)

        def __next__(self):
            return next(self)

    class String:
        def __init__(self, min_len: int = 1, max_len: int = 30, bitgenerator: np.random.PCG64 = None, seed: int = 0):
            self.min = min_len
            self.max = max_len

            self.seed = seed
            if bitgenerator is None:
                self.bitgen = np.random.PCG64(seed)
            else:
                self.bitgen = bitgenerator
            self.rng = np.random.Generator(self.bitgen)

            self.l = np.array([char for char in string.ascii_letters])

        def __next__(self):
            return ''.join(self.rng.choice(self.l,
                                           size=(self.min if self.min == self.max else self.rng.integers(self.min, self.max))))

        def get_copy(self, bitgenerator: np.random.BitGenerator):
            return SimpleFaker.String(self.min, self.max, bitgenerator=bitgenerator)

    class Integer:
        def __init__(self, start, end, bitgenerator: np.random.PCG64 = None, seed: int = 0):
            self.start = start
            self.end = end

            self.seed = seed
            if bitgenerator is None:
                self.bitgen = np.random.PCG64(seed)
            else:
                self.bitgen = bitgenerator
            self.rng = np.random.Generator(self.bitgen)

        def __next__(self):
            return self.rng.integers(self.start, self.end)

        def get_copy(self, bitgenerator: np.random.BitGenerator):
            return SimpleFaker.Integer(self.start, self.end, bitgenerator=bitgenerator)

    class Bytes:
        def __init__(self, n: int = 1, bitgenerator: np.random.PCG64 = None, seed: int = 0):
            self.n = n

            self.seed = seed
            if bitgenerator is None:
                self.bitgen = np.random.PCG64(seed)
            else:
                self.bitgen = bitgenerator
            self.rng = np.random.Generator(self.bitgen)

        def __next__(self):
            return self.rng.bytes(self.n)

        def get_copy(self, bitgenerator: np.random.BitGenerator):
            return SimpleFaker.Bytes(self.n, bitgenerator=bitgenerator)

    class Choice:
        def __init__(self, population, weights=None, cum_weights=None, bitgenerator: np.random.PCG64 = None, seed: int = 0):
            """Return a k sized list of population elements chosen with replacement.
            If the relative weights or cumulative weights are not specified,
            the selections are made with equal probability.
            """
            self.population = population
            self.weights = weights
            self.cum_weights = cum_weights

            self.seed = seed
            if bitgenerator is None:
                self.bitgen = np.random.PCG64(seed)
            else:
                self.bitgen = bitgenerator
            self.rng = np.random.Generator(self.bitgen)

        def __next__(self):
            return self.choices(self.population, weights=self.weights, cum_weights=self.cum_weights)[0]

        def get_copy(self, bitgenerator: np.random.BitGenerator):
            return SimpleFaker.Choice(self.population, self.weights, self.cum_weights, bitgenerator=bitgenerator)

        def choices(self, population, weights=None, *, cum_weights=None, k=1):
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
            bisect = _bisect
            hi = n - 1
            return [population[bisect(cum_weights, random() * total, 0, hi)]
                    for i in itertools.repeat(None, k)]
