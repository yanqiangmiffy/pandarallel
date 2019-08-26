from pathlib import Path
import pandas as pd

from .utils import (chunk, STARTED, FINISHED_WITH_ERROR, FINISHED_WITH_SUCCESS,
                    worker)


class DataFrame:
    @staticmethod
    def reduce(result_files):
        return pd.concat([
            pd.read_pickle(result_file.name)
            for result_file in result_files
        ], copy=False)

    @staticmethod
    def apply_amap(nb_workers, request_files, result_files, pool, queue,
                   df, func, *args, **kwargs):

        def apply(df, func, *args, **kwargs):
            axis = kwargs.get("axis", 0)

            if axis == 1:
                return df.apply(func, *args, **kwargs)
            else:
                raise NotImplementedError

        axis = kwargs.get("axis", 0)
        if axis == 'index':
            axis = 0
        elif axis == 'columns':
            axis = 1

        opposite_axis = 1 - axis
        chunks = chunk(df.shape[opposite_axis], nb_workers)

        for index, chunk_ in enumerate(chunks):
            df[chunk_].to_pickle(request_files[index].name)

        workers_args = [(index, req_file.name, res_file.name, queue,
                         func, args, kwargs)
                        for index, (req_file, res_file)
                        in enumerate(zip(request_files, result_files))]

        return pool.amap(worker(apply), workers_args)

    @staticmethod
    def applymap_amap(nb_workers, request_files, result_files, pool, queue,
                      df, func, *args, **kwargs):

        def applymap(df, func, *_1, **_2):
            return df.applymap(func)

        chunks = chunk(df.shape[0], nb_workers)

        for index, chunk_ in enumerate(chunks):
            df[chunk_].to_pickle(request_files[index].name)

        workers_args = [(index, req_file.name, res_file.name, queue,
                         func, args, kwargs)
                        for index, (req_file, res_file)
                        in enumerate(zip(request_files, result_files))]

        return pool.amap(worker(applymap), workers_args)
