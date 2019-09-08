import pandas as pd
from pandarallel.utils import chunk, depickle, depickle_input_and_pickle_output


class SeriesRolling:
    @staticmethod
    @depickle
    def reduce(results):
        return pd.concat(results, copy=False)

    @staticmethod
    def chunk(nb_workers, rolling, *args, **kwargs):
        return chunk(rolling.obj.size, nb_workers, rolling.window)

    @staticmethod
    def attribute2value(rolling):
        return {attribute: getattr(rolling, attribute)
                for attribute in rolling._attributes}

    @staticmethod
    def attr_to_chunk():
        return "obj"

    @staticmethod
    @depickle_input_and_pickle_output
    def worker(series, index, attribue2value, func, *args, **kwargs):
        result = series.rolling(**attribue2value).apply(func, *args, **kwargs)

        return result if index == 0 else result[attribue2value['window']:]
