from pathlib import Path
import pandas as pd

from .utils import chunk, STARTED, FINISHED_WITH_ERROR, FINISHED_WITH_SUCCESS


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
        axis = kwargs.get("axis", 0)
        if axis == 'index':
            axis = 0
        elif axis == 'columns':
            axis = 1

        opposite_axis = 1 - axis
        chunks = chunk(df.shape[opposite_axis], nb_workers)

        for index, chunk_ in enumerate(chunks):
            df[chunk_].to_pickle(request_files[index].name)

        workers_args = [(index,
                         request_file.name, result_file.name, queue,
                         func, args, kwargs)
                        for index, (request_file, result_file)
                        in enumerate(zip(request_files, result_files))]

        return pool.amap(DataFrame.apply_worker, workers_args)

    @staticmethod
    def apply_worker(worker_args):
        (index, req_file_name, res_file_name, queue,
         func, args, kwargs) = worker_args

        try:
            df = pd.read_pickle(req_file_name)
            queue.put_nowait((index, STARTED))
            axis = kwargs.get("axis", 0)

            if axis == 1:
                res = df.apply(func, *args, **kwargs)
            else:
                raise NotImplementedError

            res.to_pickle(res_file_name)

        except:
            queue.put_nowait((index, FINISHED_WITH_ERROR))
            raise

        queue.put_nowait((index, FINISHED_WITH_SUCCESS))

    @staticmethod
    def applymap_amap(nb_workers, request_files, result_files, pool, queue,
                      df, func, *args, **kwargs):
        chunks = chunk(df.shape[0], nb_workers)

        for index, chunk_ in enumerate(chunks):
            df[chunk_].to_pickle(request_files[index].name)

        workers_args = [(index, request_file.name, result_file.name, queue,
                         func)
                        for index, (request_file, result_file)
                        in enumerate(zip(request_files, result_files))]

        return pool.amap(DataFrame.applymap_worker, workers_args)

    @staticmethod
    def applymap_worker(worker_args):
        index, req_file_name, res_file_name, queue, func = worker_args

        try:
            df = pd.read_pickle(req_file_name)
            queue.put_nowait((index, STARTED))

            res = df.applymap(func)

            res.to_pickle(res_file_name)

        except:
            queue.put_nowait((index, FINISHED_WITH_ERROR))
            raise

        queue.put_nowait((index, FINISHED_WITH_SUCCESS))
