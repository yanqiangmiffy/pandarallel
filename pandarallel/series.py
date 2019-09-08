import pandas as pd
from pandarallel.utils import chunk, depickle, depickle_input_and_pickle_output


class Series:
    @staticmethod
    @depickle
    def reduce(results):
        return pd.concat(results, copy=False)

    @staticmethod
    def chunk(nb_workers, series, *args, **kwargs):
        return chunk(series.size, nb_workers)

    class Apply:
        @staticmethod
        @depickle_input_and_pickle_output
        def worker(series, _1, _2, func, *args, **kwargs):
            return series.apply(func, *args, **kwargs)

    class Map:
        @staticmethod
        @depickle_input_and_pickle_output
        def worker(series, _1, _2, func, *_, **kwargs):
            return series.map(func, **kwargs)
