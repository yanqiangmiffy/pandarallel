import dill
import os
import pandas as pd
from multiprocessing import Pool, cpu_count
from tempfile import NamedTemporaryFile
import pickle


from pandarallel.dataframe import DataFrame as DF
from pandarallel.series import Series as S
from pandarallel.series_rolling import SeriesRolling as SR
from pandarallel.dataframe_groupby import DataFrameGroupBy as DFGB
from pandarallel.rolling_groupby import RollingGroupBy as RGB

NB_WORKERS = cpu_count()
PREFIX = 'pandarallel_'
PREFIX_INPUT = PREFIX + 'input_'
PREFIX_OUTPUT = PREFIX + 'output_'
SUFFIX = '.pickle'
MEMORY_FS_ROOT = '/dev/shm'

_func = None


def worker_init(func):
    global _func
    _func = func


def global_worker(x):
    return _func(x)


def is_memory_fs_available():
    return os.path.exists(MEMORY_FS_ROOT)


def prepare_worker_memory_fs(function):
    def wrapper(worker_args):

        (input_file_path, output_file_path, index, meta_args,
         dilled_func, args, kwargs) = worker_args

        with open(input_file_path, 'rb') as file:
            data = pickle.load(file)

            # TODO: Find a better way to remove input file when not needed any
            #       more
            os.remove(input_file_path)

        result = function(data, index, meta_args,
                          dill.loads(dilled_func), *args, **kwargs)
        with open(output_file_path, 'wb') as file:
            pickle.dump(result, file)

    return wrapper


def dedill_func(function):
    def wrapper(worker_args):
        data, index, meta_args, dilled_func, args, kwargs = worker_args
        return function(data, index, meta_args,
                        dill.loads(dilled_func), *args, **kwargs)

    return wrapper


def worker(function):
    def closure(worker_args):
        (data, func, args, kwargs) = worker_args

        return function(data, func, *args, **kwargs)

    return closure


def parallelize(nb_workers, use_memory_fs, get_chunks, worker, reduce,
                get_worker_meta_args=lambda _: dict(),
                get_reduce_meta_args=lambda _: dict()):
    def closure(data, func, *args, **kwargs):
        chunks = get_chunks(nb_workers, data, *args, **kwargs)
        worker_meta_args = get_worker_meta_args(data)
        reduce_meta_args = get_reduce_meta_args(data)

        if use_memory_fs:
            input_files = [NamedTemporaryFile(prefix=PREFIX_INPUT,
                                              suffix=SUFFIX,
                                              dir=MEMORY_FS_ROOT)
                           for _ in range(nb_workers)]

            output_files = [NamedTemporaryFile(prefix=PREFIX_OUTPUT,
                                               suffix=SUFFIX,
                                               dir=MEMORY_FS_ROOT)
                            for _ in range(nb_workers)]

            try:
                for chunk, input_file in zip(chunks, input_files):
                    with open(input_file.name, 'wb') as file:
                        pickle.dump(chunk, file)

                workers_args = [(input_file.name, output_file.name, index,
                                 worker_meta_args,
                                 dill.dumps(func), args, kwargs)
                                for index, (input_file, output_file)
                                in enumerate(zip(input_files, output_files))]

                with Pool(nb_workers, worker_init,
                          (prepare_worker_memory_fs(worker),)) as pool:
                    pool.map(global_worker, workers_args)

                results = [pickle.load(output_files)
                           for output_files
                           in output_files]

                return reduce(results, reduce_meta_args)

            finally:
                # TODO: Find a better way to remove input file when not needed
                #       any more
                for file in input_files + output_files:
                    try:
                        file.close()
                    except FileNotFoundError:
                        # Probably an input file already deleted by a worker
                        pass

        else:
            workers_args = [(chunk, index, worker_meta_args,
                             dill.dumps(func), args, kwargs)
                            for index, chunk in enumerate(chunks)]

            with Pool(nb_workers, worker_init, (dedill_func(worker),)) as pool:
                results = pool.map(global_worker, workers_args)

            return reduce(results, reduce_meta_args)

    return closure


class pandarallel:
    @classmethod
    def initialize(cls, shm_size_mb=None, nb_workers=NB_WORKERS,
                   progress_bar=False, verbose=2,
                   use_memory_fs_is_available=True):
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

        use_memory_fs_is_available: bool, optional
            If possible, use memory file system to tranfer data to work on
            (dataframe, series...) between the main process and workers instead
            of pipe.

            Setting this option to True reduce data transfer time between,
            especially for big data.

            This option has an impact only if the directory `/dev/shm` exists
            and if the user has read an write rights on it.
            So basicaly this option only has an impact on some Linux
            distributions (including Ubuntu). For all others operating systems,
            standard multiprocessing data transfer (pipe) will be used
            whatever its value.
        """
        print("Pandarallel will run on", nb_workers, "workers")

        if use_memory_fs_is_available:
            if is_memory_fs_available():
                use_memory_fs = True
            else:
                print("Memory File System not available")
                use_memory_fs = False
        else:
            use_memory_fs = False

        nbw = nb_workers

        # DataFrame
        args_df_p_a = (nbw, use_memory_fs, DF.Apply.get_chunks,
                       DF.Apply.worker, DF.reduce)
        args_df_p_am = (nbw, use_memory_fs, DF.ApplyMap.get_chunks,
                        DF.ApplyMap.worker, DF.reduce)

        pd.DataFrame.parallel_apply = parallelize(*args_df_p_a)
        pd.DataFrame.parallel_applymap = parallelize(*args_df_p_am)

        # Series
        args_s_p_a = nbw, use_memory_fs, S.get_chunks, S.Apply.worker, S.reduce
        args_s_p_m = nbw, use_memory_fs, S.get_chunks, S.Map.worker, S.reduce
        pd.Series.parallel_apply = parallelize(*args_s_p_a)
        pd.Series.parallel_map = parallelize(*args_s_p_m)

        # Series Rolling
        args_sr_p_a = (nbw, use_memory_fs, SR.get_chunks, SR.worker, SR.reduce,
                       SR.attribute2value)
        pd.core.window.Rolling.parallel_apply = parallelize(*args_sr_p_a)

        # DataFrame GroupBy
        args_dfgb_p_a = (nbw, use_memory_fs, DFGB.get_chunks, DFGB.worker,
                         DFGB.reduce)
        pd.core.groupby.DataFrameGroupBy.parallel_apply = parallelize(
            *args_dfgb_p_a, get_reduce_meta_args=DFGB.get_index)

        # Rolling GroupBy
        args_rgb_p_a = (nbw, use_memory_fs, RGB.get_chunks, RGB.worker,
                        RGB.reduce, RGB.attribute2value)
        pd.core.window.RollingGroupby.parallel_apply = parallelize(
            *args_rgb_p_a)
