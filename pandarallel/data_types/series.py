import pandas as pd
from pandarallel.utils.tools import chunk


class Series:
    @staticmethod
    def reduce(results, _):
        return pd.concat(results, copy=False)

    @staticmethod
    def get_chunks(nb_workers, series, *args, **kwargs):
        for chunk_ in chunk(series.size, nb_workers):
            yield series[chunk_]

    class Apply:
        @staticmethod
        def worker(series, _1, _2, func, *args, **kwargs):
            return series.apply(func, *args, **kwargs)

    class Map:
        @staticmethod
        def worker(series, _1, _2, func, *_, **kwargs):
            return series.map(func, **kwargs)
