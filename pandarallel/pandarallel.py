import pandas as pd
from pathos.multiprocessing import ProcessingPool, cpu_count
import pickle

from pandarallel.dataframe import DataFrame as DF
from pandarallel.series import Series as S
from pandarallel.series_rolling import SeriesRolling as SR
from pandarallel.dataframe_groupby import DataFrameGroupBy as DFGB

NB_WORKERS = cpu_count()


def parallelize(nb_workers, get_chunks, worker, reduce,
                get_worker_meta_args=lambda _: dict(),
                get_reduce_meta_args=lambda _: dict()):
    def closure(data, func, *args, **kwargs):
        chunks = get_chunks(nb_workers, data, *args, **kwargs)
        worker_meta_args = get_worker_meta_args(data)
        reduce_meta_args = get_reduce_meta_args(data)

        workers_args = [(pickle.dumps(chunk),
                         index, worker_meta_args,
                         func, args, kwargs)
                        for index, chunk in enumerate(chunks)]

        with ProcessingPool(nb_workers) as pool:
            pickled_results = pool.map(worker, workers_args)

        return reduce(pickled_results, reduce_meta_args)

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

        args_df_p_a = nbw, DF.Apply.get_chunks, DF.Apply.worker, DF.reduce
        args_df_p_am = nbw, DF.ApplyMap.get_chunks, DF.ApplyMap.worker, DF.reduce
        pd.DataFrame.parallel_apply = parallelize(*args_df_p_a)
        pd.DataFrame.parallel_applymap = parallelize(*args_df_p_am)

        args_s_p_a = nbw, S.get_chunks, S.Apply.worker, S.reduce
        args_s_p_m = nbw, S.get_chunks, S.Map.worker, S.reduce
        pd.Series.parallel_apply = parallelize(*args_s_p_a)
        pd.Series.parallel_map = parallelize(*args_s_p_m)
        args_sr_p_a = (nbw, SR.get_chunks, SR.worker, SR.reduce,
                       SR.attribute2value)
        pd.core.window.Rolling.parallel_apply = parallelize(*args_sr_p_a)

        args_dfgb_p_a = nbw, DFGB.get_chunks, DFGB.worker, DFGB.reduce
        pd.core.groupby.DataFrameGroupBy.parallel_apply = parallelize(
            *args_dfgb_p_a, get_reduce_meta_args=DFGB.get_index)
