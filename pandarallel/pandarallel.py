from ctypes import c_uint8
import dill
from inspect import getsourcelines
from itertools import count
from multiprocessing import Manager, Pool, cpu_count
import os
from pandas import DataFrame, Series
from pandas.core.window import Rolling
from pandas.core.groupby import DataFrameGroupBy
from pandas.core.window import RollingGroupby
import pickle
import re
from tempfile import NamedTemporaryFile
from types import FunctionType, CodeType

from pandarallel.data_types.dataframe import DataFrame as DF
from pandarallel.data_types.series import Series as S
from pandarallel.data_types.series_rolling import SeriesRolling as SR
from pandarallel.data_types.dataframe_groupby import DataFrameGroupBy as DFGB
from pandarallel.data_types.rolling_groupby import RollingGroupBy as RGB
from pandarallel.utils.progress_bars import ProgressBarsNotebookLab

NB_WORKERS = cpu_count()
PREFIX = "pandarallel_"
PREFIX_INPUT = PREFIX + "input_"
PREFIX_OUTPUT = PREFIX + "output_"
SUFFIX = ".pickle"
MEMORY_FS_ROOT = "/dev/shm"

INPUT_FILE_READ, PROGRESSION, VALUE, ERROR = list(range(4))

_func = None


def worker_init(func):
    global _func
    _func = func


def global_worker(x):
    return _func(x)


def is_memory_fs_available():
    return os.path.exists(MEMORY_FS_ROOT)


def prepare_worker(use_memory_fs):
    def closure(function):
        def wrapper(worker_args):
            if use_memory_fs:
                (
                    input_file_path,
                    output_file_path,
                    index,
                    meta_args,
                    queue,
                    dilled_func,
                    args,
                    kwargs,
                ) = worker_args
                try:
                    with open(input_file_path, "rb") as file:
                        data = pickle.load(file)
                        queue.put((INPUT_FILE_READ, index))

                    result = function(
                        data, index, meta_args, dill.loads(dilled_func), *args, **kwargs
                    )

                    with open(output_file_path, "wb") as file:
                        pickle.dump(result, file)

                    queue.put((VALUE, index))

                except Exception:
                    queue.put((ERROR, index))
                    raise
            else:
                (data, index, meta_args, queue, dilled_func, args, kwargs) = worker_args

                try:
                    result = function(
                        data, index, meta_args, dill.loads(dilled_func), *args, **kwargs
                    )

                    queue.put((VALUE, index))

                    return result

                except Exception:
                    queue.put((ERROR, index))
                    raise

        return wrapper

    return closure


def create_temp_files(nb_files):
    return [
        NamedTemporaryFile(prefix=PREFIX_INPUT, suffix=SUFFIX, dir=MEMORY_FS_ROOT)
        for _ in range(nb_files)
    ]


# def wrap(context, progress_bar, index, queue, period):
#     context["pandarallel_counter"] = count()
#     context["pandarallel_queue"] = queue

#     def wrapper(func):
#         if progress_bar:
#             to_add = """
#     iteration = next(pandarallel_counter)
#     if not iteration % {period}:
#         pandarallel_queue.put_nowait(({progression}, ({index}, iteration)))
# """.format(
#                 period=period, progression=PROGRESSION, index=index
#             )

#             wrapped_func_source = inliner_trick(func, to_add)

#             exec(wrapped_func_source, context)
#             return context["progress_func"]

#         return func

#     return wrapper


def get_workers_args(
    context,
    use_memory_fs,
    nb_workers,
    progress_bar,
    chunks,
    worker_meta_args,
    queue,
    func,
    args,
    kwargs,
):
    def dump_and_get_lenght(chunk, input_file):
        with open(input_file.name, "wb") as file:
            pickle.dump(chunk, file)

        return len(chunk)

    if use_memory_fs:
        input_files = create_temp_files(nb_workers)
        output_files = create_temp_files(nb_workers)

        chunk_lengths = [
            dump_and_get_lenght(chunk, input_file)
            for chunk, input_file in zip(chunks, input_files)
        ]

        workers_args = [
            (
                input_file.name,
                output_file.name,
                index,
                worker_meta_args,
                queue,
                dill.dumps(func),
                args,
                kwargs,
            )
            for index, (input_file, output_file, chunk_length) in enumerate(
                zip(input_files, output_files, chunk_lengths)
            )
        ]

        return workers_args, chunk_lengths, input_files, output_files

    else:
        workers_args, chunk_lengths = zip(
            *[
                (
                    (
                        chunk,
                        index,
                        worker_meta_args,
                        queue,
                        dill.dumps(func),
                        args,
                        kwargs,
                    ),
                    len(chunk),
                )
                for index, chunk in enumerate(chunks)
            ]
        )

        return workers_args, chunk_lengths, [], []


def get_workers_result(
    use_memory_fs,
    nb_workers,
    show_progress_bar,
    queue,
    chunk_lengths,
    input_files,
    output_files,
    map_result,
):

    if show_progress_bar:
        progress_bars = ProgressBarsNotebookLab(chunk_lengths)
        progresses = [0] * nb_workers

    finished_workers = [False] * nb_workers

    generation = 0

    while not all(finished_workers):
        message_type, message = queue.get()

        if message_type is INPUT_FILE_READ:
            file_index = message
            input_files[file_index].close()

        elif message_type is PROGRESSION:
            worker_index, progression = message
            progresses[worker_index] = progression

            if generation % nb_workers == 0:
                progress_bars.update(progresses)

            generation += 1

        elif message_type is VALUE:
            worker_index = message
            finished_workers[worker_index] = VALUE

            if show_progress_bar:
                progresses[worker_index] = chunk_lengths[worker_index]
                progress_bars.update(progresses)

        elif message_type is ERROR:
            worker_index = message
            finished_workers[worker_index] = ERROR

            if show_progress_bar:
                progress_bars.set_error(worker_index)
                progress_bars.update(progresses)

    results = map_result.get()

    return (
        [pickle.load(output_files) for output_files in output_files]
        if use_memory_fs
        else results
    )


def parallelize(
    nb_workers,
    use_memory_fs,
    progress_bar,
    context,
    get_chunks,
    worker,
    reduce,
    get_worker_meta_args=lambda _: dict(),
    get_reduce_meta_args=lambda _: dict(),
):
    def closure(data, func, *args, **kwargs):
        chunks = get_chunks(nb_workers, data, *args, **kwargs)
        worker_meta_args = get_worker_meta_args(data)
        reduce_meta_args = get_reduce_meta_args(data)
        manager = Manager()
        queue = manager.Queue()

        workers_args, chunk_lengths, input_files, output_files = get_workers_args(
            context,
            use_memory_fs,
            nb_workers,
            progress_bar,
            chunks,
            worker_meta_args,
            queue,
            func,
            args,
            kwargs,
        )
        try:
            pool = Pool(
                nb_workers, worker_init, (prepare_worker(use_memory_fs)(worker),)
            )

            map_result = pool.map_async(global_worker, workers_args)

            results = get_workers_result(
                use_memory_fs,
                nb_workers,
                progress_bar,
                queue,
                chunk_lengths,
                input_files,
                output_files,
                map_result,
            )

            return reduce(results, reduce_meta_args)

        finally:
            if use_memory_fs:
                for file in input_files + output_files:
                    file.close()

    return closure


class pandarallel:
    @classmethod
    def initialize(
        cls,
        context,
        shm_size_mb=None,
        nb_workers=NB_WORKERS,
        progress_bar=False,
        verbose=1,
        use_memory_fs=None,
    ):
        """
        Initialize Pandarallel shared memory.

        Parameters
        ----------
        shm_size_mb: int, optional
            Deprecated

        nb_workers: int, optional
            Number of workers used for parallelisation

        progress_bar: bool, optional
            Display a progress bar
            WARNING: Progress bar is an experimental feature.
                     This can lead to a considerable performance loss.

        verbose: int, optional
            If verbose >= 1, display all logs
            If verbose < 1, display no log

        use_memory_fs: bool, optional
            If set to None, will use memory file system to tranfer data between
            the main process and workers if available, else will use standard
            multiprocessing data transfer (pipe).

            If set to True, will use memory file system to tranfer data between
            the main process and workers and raise a SystemError if memory file
            system is not available.

            If set to False, will use standard multiprocessing data transfer
            (pipe) to tranfer data between the main process and workers.

            Memory file system reduces data transfer time between the main
            process and workers, especially for big data.

            Memory file system is considered as available only if the
            directory `/dev/shm` exists and if the user has read an write
            rights on it.

            Basicaly memory file system is only available on some Linux
            distributions (including Ubuntu)
        """

        memory_fs_available = is_memory_fs_available()
        use_memory_fs = use_memory_fs or use_memory_fs is None and memory_fs_available

        if use_memory_fs and not memory_fs_available:
            raise SystemError("Memory file system is not available")

        if verbose >= 1:
            print("Pandarallel will run on", nb_workers, "workers.")

            if use_memory_fs:
                print(
                    "Pandarallel will use Memory file system to transfer",
                    "data between the main process and workers.",
                    sep=" ",
                )
            else:
                print(
                    "Pandarallel will use standard multiprocessing data",
                    "transfer (pipe) to transfer data between the main",
                    "process and workers.",
                    sep=" ",
                )

        nbw = nb_workers

        bargs = (nbw, use_memory_fs, progress_bar, context)
        bargs0 = (nbw, use_memory_fs, False, context)

        # DataFrame
        args = bargs + (DF.Apply.get_chunks, DF.Apply.worker, DF.reduce)
        DataFrame.parallel_apply = parallelize(*args)

        args = bargs + (DF.ApplyMap.get_chunks, DF.ApplyMap.worker, DF.reduce)
        DataFrame.parallel_applymap = parallelize(*args)

        # Series
        args = bargs + (S.get_chunks, S.Apply.worker, S.reduce)
        Series.parallel_apply = parallelize(*args)

        args = bargs + (S.get_chunks, S.Map.worker, S.reduce)
        Series.parallel_map = parallelize(*args)

        # Series Rolling
        args = bargs + (SR.get_chunks, SR.worker, SR.reduce)
        kwargs = dict(get_worker_meta_args=SR.att2value)
        Rolling.parallel_apply = parallelize(*args, **kwargs)

        # DataFrame GroupBy
        args = bargs0 + (DFGB.get_chunks, DFGB.worker, DFGB.reduce)
        kwargs = dict(get_reduce_meta_args=DFGB.get_index)
        DataFrameGroupBy.parallel_apply = parallelize(*args, **kwargs)

        # Rolling GroupBy
        args = bargs + (RGB.get_chunks, RGB.worker, RGB.reduce)
        kwargs = dict(get_worker_meta_args=SR.att2value)
        RollingGroupby.parallel_apply = parallelize(*args, **kwargs)
