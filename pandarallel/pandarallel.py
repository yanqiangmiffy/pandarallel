import pandas as pd
import multiprocessing as multiprocessing

from .dataframe import DataFrame

NB_WORKERS = multiprocessing.cpu_count()


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

        pd.DataFrame.parallel_apply = DataFrame.apply(nb_workers)
