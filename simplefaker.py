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
            self.value = value

        def __next__(self):
            return self.value

    class Sequence:
        """Iterator that counts upward forever."""

        def __init__(self, start=0):
            self.start = start

        def __next__(self):
            start = self.start
            self.start += 1
            return start


    class UUIDv4:
        def __init__(self, bitgenerator: np.random.PCG64):
            self.bitgen = np.random.PCG64() if bitgenerator is None else bitgenerator

            self.rng = np.random.Generator(self.bitgen)

        def __next__(self):
            return uuid.UUID(bytes=self.rng.bytes(16), version=4)


    class Timestamp:
        def __init__(self, start: str, end: str, bitgenerator: np.random.PCG64, format: str):
            self.format = '%Y-%m-%d %H:%M:%S.%f' if format is None else format
            self.start = dt.datetime.fromisoformat(start).timestamp() * 1000000
            self.end = dt.datetime.fromisoformat(end).timestamp() * 1000000

            self.bitgen = np.random.PCG64() if bitgenerator is None else bitgenerator

            self.rng = np.random.Generator(self.bitgen)
            
        def __next__(self):
            return dt.datetime.fromtimestamp(self.rng.integers(self.start, self.end)/1000000).strftime(self.format)


    class Date(Timestamp):
        def __init__(self, start: str, end: str, bitgenerator: np.random.PCG64, format: str = '%Y-%m-%d'):
            self.format = '%Y-%m-%d' if format is None else format
            super().__init__(start=start, end=end, bitgenerator=bitgenerator, format=self.format)

    class Time(Timestamp):
        def __init__(self, start: str, end: str, bitgenerator: np.random.PCG64, micros: bool):
            self.format = '%H:%M:%S.%f' if micros is True else '%H:%M:%S'
            super().__init__(start='1970-01-01 ' + start,
                             end='1970-01-01 ' + end, bitgenerator=bitgenerator, format=self.format)

    class String:
        def __init__(self, min: int, max: int, bitgenerator: np.random.PCG64):
            self.min: int = 10 if min is None else min
            self.max: int = 50 if max is None else max
            self.letters: np.array = np.array([char for char in string.ascii_letters])
            self.bitgen = np.random.PCG64() if bitgenerator is None else bitgenerator

            self.rng = np.random.Generator(self.bitgen)

        def __next__(self):
            return ''.join(self.rng.choice(self.letters,
                                           size=(self.min if self.min == self.max else self.rng.integers(self.min, self.max))))


    class Integer:
        def __init__(self, min: int, max: int, bitgenerator: np.random.PCG64):
            self.min: int = 0 if min is None else min 
            self.max: int = 1000000 if max is None else max          
            self.bitgen = np.random.PCG64() if bitgenerator is None else bitgenerator

            self.rng = np.random.Generator(self.bitgen)

        def __next__(self):
            return self.rng.integers(self.min, self.max)

    class Bytes:
        def __init__(self, n: int, bitgenerator: np.random.PCG64):
            self.n: int = 1 if n is None else n

            self.bitgen = np.random.PCG64() if bitgenerator is None else bitgenerator

            self.rng = np.random.Generator(self.bitgen)

        def __next__(self):
            return self.rng.bytes(self.n)

    class Choice:
        def __init__(self, population, bitgenerator: np.random.PCG64, weights=None, cum_weights=None):
            """Return a k sized list of population elements chosen with replacement.
            If the relative weights or cumulative weights are not specified,
            the selections are made with equal probability.
            """
            self.population = population
            self.weights = weights
            self.cum_weights = cum_weights

            self.bitgen = np.random.PCG64() if bitgenerator is None else bitgenerator

            self.rng = np.random.Generator(self.bitgen)

        def __next__(self):
            return self.choices(self.population, weights=self.weights, cum_weights=self.cum_weights)[0]

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
