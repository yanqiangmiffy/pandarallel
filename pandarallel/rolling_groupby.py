import itertools
import pandas as pd
from pandarallel.utils import chunk


class RollingGroupBy:
    @staticmethod
    def reduce(results, _):
        return pd.concat(results, copy=False)

    @staticmethod
    def get_chunks(nb_workers, rolling_groupby, *args, **kwargs):
        chunks = chunk(len(rolling_groupby._groupby), nb_workers)
        iterator = iter(rolling_groupby._groupby)

        for chunk_ in chunks:
            yield [next(iterator) for _ in range(chunk_.stop - chunk_.start)]

    @staticmethod
    def attribute2value(rolling):
        return {attribute: getattr(rolling, attribute)
                for attribute in rolling._attributes}

    @staticmethod
    def worker(tuples, _, attribute2value, func, *args, **kwargs):
        # TODO: See if this pd.concat is avoidable
        results = []

        for name, df in tuples:
            item = df.rolling(**attribute2value).apply(func, *args, **kwargs)
            item.index = pd.MultiIndex.from_product([[name], item.index])
            results.append(item)

        return pd.concat(results)
