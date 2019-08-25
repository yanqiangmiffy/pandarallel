from pathlib import Path
import pandas as pd
from multiprocessing import Manager
from pathos.multiprocessing import ProcessingPool
from tempfile import NamedTemporaryFile
from .utils import chunk

DIR = '/dev/shm'
PREFIX = 'pandarallel_'
SUFFIX_REQUEST = '_request.pkl'
SUFFIX_RESULT = '_result.pkl'

STARTED, FINISHED_WITH_SUCCESS, FINISHED_WITH_ERROR = 0, -1, -2


class DataFrame:
    @staticmethod
    def worker_apply(args):
        index, req_file_name, res_file_name, queue, func, args, kwargs = args

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
    def apply(nb_workers):
        def closure(df, func, *args, **kwargs):
            pool = ProcessingPool(nb_workers)
            manager = Manager()
            queue = manager.Queue()

            finished_workers = [False] * nb_workers

            request_files = [NamedTemporaryFile(dir=DIR,
                                                prefix=PREFIX,
                                                suffix=SUFFIX_REQUEST)
                             for _ in range(nb_workers)]

            result_files = [NamedTemporaryFile(dir=DIR,
                                               prefix=PREFIX,
                                               suffix=SUFFIX_RESULT)
                            for _ in range(nb_workers)]

            try:
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

                results = pool.amap(DataFrame.worker_apply, workers_args)

                while not all(finished_workers):
                    index, status = queue.get()

                    if status is STARTED:
                        request_files[index].close()
                    elif status is FINISHED_WITH_SUCCESS:
                        finished_workers[index] = True
                    elif status is FINISHED_WITH_ERROR:
                        # TODO: Find something to stop all workers as soon as
                        #       an exception is raised on one of the workers
                        finished_workers[index] = True

                # This method call is here only to forward potential worker
                # exception to the user
                results.get()

                result = pd.concat([
                    pd.read_pickle(result_file.name)
                    for result_file in result_files
                ], copy=False)

                return result

            finally:
                for file in request_files + result_files:
                    file.close()

        return closure

    @staticmethod
    def worker_applymap(args):
        index, req_file_name, res_file_name, queue, func = args
