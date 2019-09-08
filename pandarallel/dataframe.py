import pandas as pd
from pandarallel.utils import chunk, depickle, depickle_input_and_pickle_output


class DataFrame:
    @staticmethod
    @depickle
    def reduce(results):
        return pd.concat(results, copy=False)

    @staticmethod
    def apply_chunk(nb_workers, df, *args, **kwargs):
        axis = kwargs.get("axis", 0)
        if axis == 'index':
            axis = 0
        elif axis == 'columns':
            axis = 1

        opposite_axis = 1 - axis
        chunks = chunk(df.shape[opposite_axis], nb_workers)

        return chunks

    @staticmethod
    @depickle_input_and_pickle_output
    def apply_worker(df, func, *args, **kwargs):
        axis = kwargs.get("axis", 0)

        if axis == 1:
            return df.apply(func, *args, **kwargs)
        else:
            raise NotImplementedError

    @staticmethod
    def applymap_chunk(nb_workers, df, *_):
        return chunk(df.shape[0], nb_workers)

    @staticmethod
    @depickle_input_and_pickle_output
    def applymap_worker(df, func, *_):
        return df.applymap(func)
