from inspect import signature
import re
from types import CodeType, FunctionType
from typing import Any, Dict, Tuple


class OpCode:
    LOAD_ATTR = b"j"
    LOAD_CONST = b"d"
    LOAD_FAST = b"|"
    LOAD_GLOBAL = b"t"
    LOAD_METHOD = b"\xa0"
    RETURN_VALUE = b"S"
    STORE_ATTR = b"_"
    STORE_FAST = b"}"


def multiple_find(bytecode: bytes, sub_bytecode: bytes) -> Tuple[int, ...]:
    """Return list of index where `sub_bytecode` is found in `bytecode`.
    Example: With bytecode = b'\x01\x03\x04\x02\x01\x05\x01' and sub_bytecode = b'\x01',
             the expected output is (0, 4, 6)
    """
    return tuple(m.start() for m in re.finditer(sub_bytecode, bytecode))


def multiple_replace(bytecode: bytes, rep_dict: Dict[bytes, bytes]):
    """Replace, in the bytecode `bytecode`, each key of `rep_dict` by the corresponding
    value of `rep_dict`.

    Example: bytecode = b"\x01\x02\x03\x01"
             rep_dict = {b"\x01": b"\x02", b"\x02": b"\x03"}

             The returned string is: b"\x02\x03\x03\x02"
    """

    pattern = re.compile(
        b"|".join([re.escape(k) for k in sorted(rep_dict, key=len, reverse=True)]),
        flags=re.DOTALL,
    )
    return pattern.sub(lambda x: rep_dict[x.group(0)], bytecode)


def remove_duplicates(tuple_: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """Remove duplicate in tuple `tuple_`.

    Example: tuple_ = (3, 1, 2, 2, 1, 4)
             The returned tuple is: (3, 1, 2, 4)
    """

    return tuple(sorted(set(tuple_), key=tuple_.index))


def get_hex(number: int) -> str:
    """Convert decimal number to hex string with pre-0 if the lenght of the hex string
    is odd.

    Examples: get_hex(3) == "03"
              get_hex(16) == "10"
              get_hex(1000) = "03e8"

    If `number` is negative, raise a ValueError.
    """
    if number < 0:
        raise ValueError("`number` is negative")

    hex_number = hex(number)[2:]

    if len(hex_number) % 2 != 0:
        return "0{}".format(hex_number)

    return hex_number


def get_bytecode(number: int) -> bytes:
    """Convert integer to bytecode.
    If `number` < 0 raise a ValueError.

    Examples:
    get_bytecode(3) == b'\x03'
    get_bytecode(10) == b'\x0A'
    get_bytecode(42) == b'\x2A'
    """
    if number < 0:
        raise ValueError("`number` is negative")

    hex_number = get_hex(number)

    return bytes.fromhex(hex_number)


def has_no_return(func: FunctionType) -> bool:
    """Return True if `func` returns nothing, else return False"""

    code = func.__code__

    co_code = code.co_code
    co_consts = code.co_consts

    load_const_none = OpCode.LOAD_CONST + get_bytecode(co_consts.index(None))

    return_indexes = multiple_find(co_code, OpCode.RETURN_VALUE)

    return (
        len(return_indexes) == 1
        and co_code[return_indexes[0] - 2 : return_indexes[0]] == load_const_none
    )


def has_duplicates(tuple_: Tuple):
    """Return True if `tuple_` contains duplicates items.

    Exemple: has_duplicates((1, 3, 2, 4)) == False
             has_duplicates((1, 3, 2, 3)) == True
    """

    return len(set(tuple_)) != len(tuple_)


def key2value(sources: Tuple, dests: Tuple, source2dest: Dict) -> Tuple:
    """Delete all items in `sources` tuple which are in `source2dest` keys and add
    corresponding values in `dests` keys.

    Example:
    sources = ("a", "b", "d", "hello")
    dests = (54, "e", 2)
    source2dest = {"b": 55, "hello": "world"}

    key2value(sources, dests, source2dest) = (("a", "d"),
                                              (54, "e", 2, 55, "world),
                                              {"1": "3", "3": "4"})

    `sources` and `dests` should not have duplicate items, else a ValueError is raided.
    All keys of `source2dest` should be in `sources`, else a ValueError is raised.
    """
    if has_duplicates(sources):
        raise ValueError("`sources` has duplicates")

    if has_duplicates(dests):
        raise ValueError("`dests` has duplicates")

    if len(set(source2dest) - set(sources)) != 0:
        raise ValueError("Some keys in `source2dest` are not in `sources`")

    new_sources = tuple(item for item in sources if item not in source2dest)
    new_dests = remove_duplicates(dests + tuple(source2dest.values()))

    transitions = {
        sources.index(key): new_dests.index(value) for key, value in source2dest.items()
    }

    return new_sources, new_dests, transitions


def get_transitions(olds: Tuple, news: Tuple) -> Dict[int, int]:
    """Returns a dictionnary where a key represents a position of an item in olds and
    a value represents the position of the same item in news.

    If an element of `olds` is not in `news`, then the corresponding value will be
    `None`.

    Exemples:
    olds = ("a", "c", "b", "d")
    news_1 = ("f", "g", "c", "d", "b", "a")
    news_2 = ("c", "d")

    get_transitions(olds, news_1) = {0: 5, 1: 2, 2: 4, 3: 3}
    get_transitions(olds, news_2) = {1: 0, 3: 1}

    `olds` and `news` should not have any duplicates, else a ValueError is raised.
    """
    if has_duplicates(olds):
        raise ValueError("`olds` has duplicates")

    if has_duplicates(news):
        raise ValueError("`news` has duplicates")

    return {
        index_old: news.index(old)
        for index_old, old in [(olds.index(old), old) for old in olds if old in news]
    }


def get_b_transitions(
    transitions: Dict[int, int], byte_source: bytes, byte_dest: bytes
):
    return {
        byte_source + get_bytecode(key): byte_dest + get_bytecode(value)
        for key, value in transitions.items()
    }


def are_functions_equivalent(l_func, r_func):
    """Return True if `l_func` and `r_func` are equivalent"""
    l_code, r_code = l_func.__code__, r_func.__code__

    trans_co_consts = get_transitions(l_code.co_consts, r_code.co_consts)
    trans_co_names = get_transitions(l_code.co_names, r_code.co_names)
    trans_co_varnames = get_transitions(l_code.co_varnames, r_code.co_varnames)

    transitions = {
        **get_b_transitions(trans_co_consts, OpCode.LOAD_CONST, OpCode.LOAD_CONST),
        **get_b_transitions(trans_co_names, OpCode.LOAD_GLOBAL, OpCode.LOAD_GLOBAL),
        **get_b_transitions(trans_co_names, OpCode.LOAD_METHOD, OpCode.LOAD_METHOD),
        **get_b_transitions(trans_co_names, OpCode.LOAD_ATTR, OpCode.LOAD_ATTR),
        **get_b_transitions(trans_co_names, OpCode.STORE_ATTR, OpCode.STORE_ATTR),
        **get_b_transitions(trans_co_varnames, OpCode.LOAD_FAST, OpCode.LOAD_FAST),
        **get_b_transitions(trans_co_varnames, OpCode.STORE_FAST, OpCode.STORE_FAST),
    }

    new_l_co_code = multiple_replace(l_code.co_code, transitions)

    co_code_cond = new_l_co_code == r_code.co_code
    co_consts_cond = set(l_code.co_consts) == set(r_code.co_consts)
    co_names_cond = set(l_code.co_names) == set(l_code.co_names)
    co_varnames_cond = set(l_code.co_varnames) == set(l_code.co_varnames)

    return co_code_cond and co_consts_cond and co_names_cond and co_varnames_cond


def pin_arguments(func: FunctionType, arguments: dict):
    """Transform `func` in a function with no arguments.

    Example:

    def func(a, b):
        c = 4
        print(str(a) + str(c))

        return b

    The function returned by pin_arguments(func, {"a": 10, "b": 11}) is equivalent to:

    def pinned_func():
        c = 4
        print(str(10) + str(c))

        return 11

    This function is in some ways equivalent to functools.partials but with a faster
    runtime.

    `arguments` keys should be identical as `func` arguments names else a Type is
    raised.
    """

    if signature(func).parameters.keys() != set(arguments):
        raise TypeError("`arguments` and `func` arguments do not correspond")

    func_code = func.__code__
    func_co_code = func_code.co_code
    func_co_consts = func_code.co_consts
    func_co_varnames = func_code.co_varnames

    new_co_consts = remove_duplicates(func_co_consts + tuple(arguments.values()))
    new_co_varnames = tuple(item for item in func_co_varnames if item not in arguments)

    trans_co_varnames2_co_consts = {
        func_co_varnames.index(key): new_co_consts.index(value)
        for key, value in arguments.items()
    }

    trans_co_varnames = get_transitions(func_co_varnames, new_co_varnames)

    transitions = {
        **get_b_transitions(
            trans_co_varnames2_co_consts, OpCode.LOAD_FAST, OpCode.LOAD_CONST
        ),
        **get_b_transitions(trans_co_varnames, OpCode.LOAD_FAST, OpCode.LOAD_FAST),
        **get_b_transitions(trans_co_varnames, OpCode.STORE_FAST, OpCode.STORE_FAST),
    }

    new_co_code = multiple_replace(func_co_code, transitions)

    new_func = FunctionType(
        func.__code__,
        func.__globals__,
        func.__name__,
        func.__defaults__,
        func.__closure__,
    )

    nfcode = new_func.__code__

    new_func.__code__ = CodeType(
        0,
        0,
        len(new_co_varnames),
        nfcode.co_stacksize,
        nfcode.co_flags,
        new_co_code,
        new_co_consts,
        nfcode.co_names,
        new_co_varnames,
        nfcode.co_filename,
        nfcode.co_name,
        nfcode.co_firstlineno,
        nfcode.co_lnotab,
        nfcode.co_freevars,
        nfcode.co_cellvars,
    )

    return new_func


def inline(pre_func: FunctionType, func: FunctionType, pre_func_arguments: dict):
    """Insert `prefunc` at the beginning of `func` and return the corresponding
    function.

    `pre_func` should not have a return statement (else a ValueError is raised).
    `pre_func_arguments` keys should be identical as `func` arguments names else a
    TypeError is raised.

    This approach takes less CPU instructions than the standard decorator approach.

    Example:

    def pre_func(b, c):
        a = "hello"
        print(a + " " + b + " " + c)

    def func(x, y):
        z = x + 2 * y
        return z ** 2

    The returned function corresponds to:

    def inlined(x, y):
        a = "hello"
        print(a)
        z = x + 2 * y
        return z ** 2
    """

    new_func = FunctionType(
        func.__code__,
        func.__globals__,
        func.__name__,
        func.__defaults__,
        func.__closure__,
    )

    if not has_no_return(pre_func):
        raise ValueError("`pre_func` returns something")

    pinned_pre_func = pin_arguments(pre_func, pre_func_arguments)

    pinned_pre_func_code = pinned_pre_func.__code__
    pinned_pre_func_co_code = pinned_pre_func_code.co_code
    pinned_pre_func_co_consts = pinned_pre_func_code.co_consts
    pinned_pre_func_co_names = pinned_pre_func_code.co_names
    pinned_pre_func_co_varnames = pinned_pre_func_code.co_varnames

    func_code = func.__code__
    func_co_code = func_code.co_code
    func_co_consts = func_code.co_consts
    func_co_names = func_code.co_names
    func_co_varnames = func_code.co_varnames

    new_co_consts = remove_duplicates(func_co_consts + pinned_pre_func_co_consts)
    new_co_names = remove_duplicates(func_co_names + pinned_pre_func_co_names)
    new_co_varnames = remove_duplicates(func_co_varnames + pinned_pre_func_co_varnames)

    trans_co_consts = get_transitions(pinned_pre_func_co_consts, new_co_consts)
    trans_co_names = get_transitions(pinned_pre_func_co_names, new_co_names)
    trans_co_varnames = get_transitions(pinned_pre_func_co_varnames, new_co_varnames)

    transitions = {
        **get_b_transitions(trans_co_consts, OpCode.LOAD_CONST, OpCode.LOAD_CONST),
        **get_b_transitions(trans_co_names, OpCode.LOAD_GLOBAL, OpCode.LOAD_GLOBAL),
        **get_b_transitions(trans_co_names, OpCode.LOAD_METHOD, OpCode.LOAD_METHOD),
        **get_b_transitions(trans_co_names, OpCode.LOAD_ATTR, OpCode.LOAD_ATTR),
        **get_b_transitions(trans_co_names, OpCode.STORE_ATTR, OpCode.STORE_ATTR),
        **get_b_transitions(trans_co_varnames, OpCode.LOAD_FAST, OpCode.LOAD_FAST),
        **get_b_transitions(trans_co_varnames, OpCode.STORE_FAST, OpCode.STORE_FAST),
    }

    pinned_pre_func_co_code = multiple_replace(pinned_pre_func_co_code, transitions)
    new_co_code = pinned_pre_func_co_code[:-4] + func_co_code

    nfcode = new_func.__code__

    new_func.__code__ = CodeType(
        nfcode.co_argcount,
        nfcode.co_kwonlyargcount,
        len(new_co_varnames),
        nfcode.co_stacksize,
        nfcode.co_flags,
        new_co_code,
        new_co_consts,
        new_co_names,
        new_co_varnames,
        nfcode.co_filename,
        nfcode.co_name,
        nfcode.co_firstlineno,
        nfcode.co_lnotab,
        nfcode.co_freevars,
        nfcode.co_cellvars,
    )

    return new_func
