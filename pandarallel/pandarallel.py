import pandas as pd
from pathos.multiprocessing import ProcessingPool, cpu_count
import pickle

from .dataframe import DataFrame as DF
from .series import Series as S

NB_WORKERS = cpu_count()


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

        args_df_p_a = nbw, DF.apply_chunk, DF.apply_worker, DF.reduce
        args_df_p_am = nbw, DF.applymap_chunk, DF.applymap_worker, DF.reduce

        pd.DataFrame.parallel_apply = parallelize(*args_df_p_a)
        pd.DataFrame.parallel_applymap = parallelize(*args_df_p_am)


def parallelize(nb_workers, get_chunks, worker, reduce):
    def closure(df, func, *args, **kwargs):
        chunks = get_chunks(nb_workers, df, *args, **kwargs)

        workers_args = [(pickle.dumps(df[chunk_]), func, args, kwargs)
                        for chunk_ in chunks]

        with ProcessingPool(nb_workers) as pool:
            pickled_results = pool.map(worker, workers_args)

        return reduce(pickled_results)

    return closure
