import pandas as pd
from pathos.multiprocessing import ProcessingPool, cpu_count
import pickle

from pandarallel.dataframe import DataFrame as DF
from pandarallel.series import Series as S
from pandarallel.series_rolling import SeriesRolling as SR

NB_WORKERS = cpu_count()


def parallelize(nb_workers, get_chunks, worker, reduce,
                get_worker_meta_args=lambda _: dict(), attr_to_work_on=None):
    def closure(data, func, *args, **kwargs):
        data_to_work_on = (getattr(data, attr_to_work_on())
                           if attr_to_work_on else data)

        chunks = get_chunks(nb_workers, data, *args, **kwargs)

        worker_meta_args = get_worker_meta_args(data)

        workers_args = [(pickle.dumps(data_to_work_on[chunk_]),
                         index, worker_meta_args,
                         func, args, kwargs)
                        for index, chunk_ in enumerate(chunks)]

        with ProcessingPool(nb_workers) as pool:
            pickled_results = pool.map(worker, workers_args)

        return reduce(pickled_results)

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
            Deprecated

        nb_workers: int, optional
            Number of workers used for parallelisation

        progress_bar: bool, optional
            Not working on this version

        verbose: int, optional
            Not working on this version
        """
        print("Pandarallel will run on", nb_workers, "workers")

        nbw = nb_workers

        args_df_p_a = nbw, DF.Apply.chunk, DF.Apply.worker, DF.reduce
        args_df_p_am = nbw, DF.ApplyMap.chunk, DF.ApplyMap.worker, DF.reduce
        pd.DataFrame.parallel_apply = parallelize(*args_df_p_a)
        pd.DataFrame.parallel_applymap = parallelize(*args_df_p_am)

        args_s_p_a = nbw, S.chunk, S.Apply.worker, S.reduce
        args_s_p_m = nbw, S.chunk, S.Map.worker, S.reduce
        pd.Series.parallel_apply = parallelize(*args_s_p_a)
        pd.Series.parallel_map = parallelize(*args_s_p_m)

        args_sr_p_a = (nbw, SR.chunk, SR.worker, SR.reduce,
                       SR.attribute2value, SR.attr_to_chunk)
        pd.core.window.Rolling.parallel_apply = parallelize(*args_sr_p_a)
