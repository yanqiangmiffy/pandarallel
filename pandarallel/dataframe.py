import pandas as pd
from pandarallel.utils import chunk, depickle, depickle_input_and_pickle_output


class DataFrame:
    @staticmethod
    @depickle
    def reduce(results, _):
        return pd.concat(results, copy=False)

    class Apply:
        @staticmethod
        def get_chunks(nb_workers, df, *args, **kwargs):
            axis = kwargs.get("axis", 0)
            if axis == 'index':
                axis = 0
            elif axis == 'columns':
                axis = 1

            opposite_axis = 1 - axis

            for chunk_ in chunk(df.shape[opposite_axis], nb_workers):
                yield df[chunk_]

        @staticmethod
        @depickle_input_and_pickle_output
        def worker(df, _1, _2, func, *args, **kwargs):
            axis = kwargs.get("axis", 0)

            if axis == 1:
                return df.apply(func, *args, **kwargs)
            else:
                raise NotImplementedError

    class ApplyMap:
        @staticmethod
        def get_chunks(nb_workers, df, *_):
            for chunk_ in chunk(df.shape[0], nb_workers):
                yield df[chunk_]

        @staticmethod
        @depickle_input_and_pickle_output
        def worker(df, _1, _2, func, *_):
            return df.applymap(func)
