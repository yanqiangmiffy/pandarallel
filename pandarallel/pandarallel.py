import pandas as pd
import multiprocessing as multiprocessing
from pathos.multiprocessing import ProcessingPool
from multiprocessing import Manager
from tempfile import NamedTemporaryFile

from .utils import STARTED, FINISHED_WITH_ERROR, FINISHED_WITH_SUCCESS
from .dataframe import DataFrame

DIR = '/dev/shm'
PREFIX = 'pandarallel_'
SUFFIX_REQUEST = '_request.pkl'
SUFFIX_RESULT = '_result.pkl'
NB_WORKERS = multiprocessing.cpu_count()


def wrapper(nb_workers, amap, reduce):
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
            results = amap(nb_workers, request_files, result_files,
                           pool, queue, df,
                           func, *args, **kwargs)

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

            return reduce(result_files)

        finally:
            for file in request_files + result_files:
                file.close()

    return closure


class pandarallel:
    @classmethod
    def initialize(cls, shm_size_mb=None, nb_workers=NB_WORKERS,
                   progress_bar=False, verbose=2):
        """
        Initialize Pandarallel shared memory.

        Parameters
        ----------
        shm_size_mb: int, optional
            Size of Pandarallel shared memory
            NON WORKING PARAMETER ON THIS VERSION

        nb_workers: int, optional
            Number of worker used for parallelisation

        progress_bar: bool, optional
            Display a progress bar
            WARNING: Progress bar is an experimental feature.
                        This can lead to a considerable performance loss.

            NON WORKING PARAMETER ON THIS VERSION

        verbose: int, optional
            If verbose >= 2, display all logs
            If verbose == 1, display only initialization logs
            If verbose < 1, display no log

            NON WORKING PARAMETER ON THIS VERSION

        """
        if progress_bar:
            print("WARNING: Progress bar is an experimental feature. This \
can lead to a considerable performance loss.")

        print("Pandarallel will run on", nb_workers, "workers")

        pd.DataFrame.parallel_apply = wrapper(nb_workers,
                                              DataFrame.apply_amap,
                                              DataFrame.reduce)

        pd.DataFrame.parallel_applymap = wrapper(nb_workers,
                                                 DataFrame.applymap_amap,
                                                 DataFrame.reduce)
