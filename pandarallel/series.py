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

    @staticmethod
    @depickle_input_and_pickle_output
    def apply_worker(series, func, *args, **kwargs):
        return series.apply(func, *args, **kwargs)

    @staticmethod
    @depickle_input_and_pickle_output
    def map_worker(series, func, *_, **kwargs):
        return series.map(func, **kwargs)