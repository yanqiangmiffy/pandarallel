from pathlib import Path
import pandas as pd

from .utils import (chunk, worker,
                    STARTED, FINISHED_WITH_ERROR, FINISHED_WITH_SUCCESS)


class Series:
    @staticmethod
    def reduce(result_files):
        return pd.concat([
            pd.read_pickle(result_file.name)
            for result_file in result_files
        ], copy=False)

    @staticmethod
    def apply_amap(nb_workers, request_files, result_files, pool, queue,
                   series, func, *args, **kwargs):

        def apply(series, func, *args, **kwargs):
            return series.apply(func, *args, **kwargs)

        chunks = chunk(series.shape[0], nb_workers)

        for index, chunk_ in enumerate(chunks):
            series[chunk_].to_pickle(request_files[index].name)

        workers_args = [(index, req_file.name, res_file.name, queue,
                         func, args, kwargs)
                        for index, (req_file, res_file)
                        in enumerate(zip(request_files, result_files))]

        return pool.amap(worker(apply), workers_args)

    @staticmethod
    def map_amap(nb_workers, request_files, result_files, pool, queue,
                 series, func, *args, **kwargs):

        def map(series, func, *_1, **_2):
            return series.map(func)

        chunks = chunk(series.shape[0], nb_workers)

        for index, chunk_ in enumerate(chunks):
            series[chunk_].to_pickle(request_files[index].name)

        workers_args = [(index, req_file.name, res_file.name, queue,
                         func, args, kwargs)
                        for index, (req_file, res_file)
                        in enumerate(zip(request_files, result_files))]

        return pool.amap(worker(map), workers_args)
