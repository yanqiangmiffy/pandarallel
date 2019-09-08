import itertools as _itertools
import pandas as pd
import pickle

STARTED, FINISHED_WITH_SUCCESS, FINISHED_WITH_ERROR = 0, -1, -2


def chunk(nb_item, nb_chunks, start_offset=0):
    """
    Return `nb_chunks` slices of approximatively `nb_item / nb_chunks` each.

    Parameters
    ----------
    nb_item : int
        Total number of items

    nb_chunks : int
        Number of chunks to return

    start_offset : int
        Shift start of slice by this amount

    Returns
    -------
    A list of slices


    Examples
    --------
    >>> chunks = _pandarallel._chunk(103, 4)
    >>> chunks
    [slice(0, 26, None), slice(26, 52, None), slice(52, 78, None),
     slice(78, 103, None)]
    """
    if nb_item <= nb_chunks:
        return [
            slice(max(0, idx - start_offset), idx + 1)
            for idx in range(nb_item)
        ]

    quotient = nb_item // nb_chunks
    remainder = nb_item % nb_chunks

    quotients = [quotient] * nb_chunks
    remainders = [1] * remainder + [0] * (nb_chunks - remainder)

    nb_elems_per_chunk = [
        quotient + remainder for quotient, remainder
        in zip(quotients, remainders)
    ]

    accumulated = list(_itertools.accumulate(nb_elems_per_chunk))
    shifted_accumulated = accumulated.copy()
    shifted_accumulated.insert(0, 0)
    shifted_accumulated.pop()

    return [
        slice(max(0, begin - start_offset), end) for begin, end
        in zip(shifted_accumulated, accumulated)
    ]


def worker(function):
    def closure(worker_args):
        (data, func, args, kwargs) = worker_args

        return function(data, func, *args, **kwargs)

    return closure

def depickle_input_and_pickle_output(function):
    def wrapper(worker_args):
        pickled_df, func, args, kwargs = worker_args

        df = pickle.loads(pickled_df)
        del(pickled_df)

        result = function(df, func, *args, **kwargs)

        return pickle.dumps(result)

    return wrapper


def depickle(function):
    def wrapper(pickled_results):
        results = [pickle.loads(pickled_result)
                   for pickled_result in pickled_results]
        return function(results)
    return wrapper