from inspect import signature
import re
from types import CodeType, FunctionType
from typing import Any, Callable, Dict, Tuple


class OpCode:
    LOAD_CONST = b"d"
    LOAD_FAST = b"|"
    LOAD_GLOBAL = b"t"
    STORE_FAST = b"}"
    RETURN_VALUE = b"S"


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


def has_freevar(func: Callable) -> bool:
    """Return True is `func` has at least one freevar (i.e. is decorated).

    Else return False.
    """
    return len(func.__code__.co_freevars) != 0


def has_no_return(func: Callable) -> bool:
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


def get_transitions(olds: Tuple[Any, ...], news: Tuple[Any, ...]) -> Dict[int, int]:
    """Returns a dictionnary where a key represents a position of an item in olds and
    a value represents the position of the same item in news.

    Exemples:
    olds = ("a", "c", "b", "d")
    news = ("f", "g", "c", "d", "b", "a")

    get_transitions(olds, news) = {0: 5, 1: 2, 2: 4, 3: 3}

    `olds` and `news` should not have any duplicates, else a ValueError is raised.
    All elements of `olds` should be in `news`, else a ValueError is raised.
    """
    if has_duplicates(olds):
        raise ValueError("`olds` has duplicates")

    if has_duplicates(news):
        raise ValueError("`news` has duplicates")

    if not set(olds) <= set(news):
        raise ValueError("At least on item of `olds` is not in `news`")

    return {index: news.index(old) for index, old in enumerate(olds)}


def get_new_func_attributes(
    pre_func: FunctionType, func: FunctionType, pre_func_arguments: Tuple[Any, ...]
) -> Tuple[bytes, Tuple[Any, ...], Tuple[Any, ...], Tuple[Any, ...]]:
    """Insert `prefunc` at the beginning of `func` and returns a co_code, co_consts,
       co_names & co_varnames of the new function.

    `pre_func` should not have a return statement (else a ValueError is raised).
    `pre_func_arguments` should contain exactly the same number of items than the
    arguments taken by `pre_func` (else a TypeError is raised)

    Example:

    def pre_func(a, b):
        c = "Hello"
        print(c + " " + a + " " + b)

    def func(x, y):
        z = x + 2 * y
        return z ** 2

    The items returned by get_new_func_attributes(pre_func, func, ("how are", "you?"))
    correspond to the following function

    def inlined(x, y):
        c = "Hello"
        print(c + " " + "how are" + " " + "you?")
        z = x + 2 * y
        return z ** 2
    """

    if not has_no_return(pre_func):
        raise ValueError("`pre_func` returns something")

    if len(signature(pre_func).parameters) == len(pre_func_arguments):
        msg = "`pre_func_arguments` and arguments in `pre_func` do not correspond"
        raise TypeError(msg)

    pre_func_code = pre_func.__code__
    pre_func_co_code = pre_func_code.co_code
    pre_func_co_consts = pre_func_code.co_consts
    pre_func_co_names = pre_func_code.co_names
    pre_func_co_varnames = pre_func_code.co_varnames

    func_code = func.__code__
    func_co_code = func_code.co_code
    func_co_consts = func_code.co_consts
    func_co_names = func_code.co_names
    func_co_varnames = func_code.co_varnames

    new_co_consts = remove_duplicates(func_co_consts + pre_func_co_consts)
    new_co_names = remove_duplicates(func_co_names + pre_func_co_names)
    new_co_varnames = remove_duplicates(func_co_varnames + pre_func_co_varnames)

    transitions_co_consts = get_transitions(pre_func_co_consts, new_co_consts)
    transitions_co_names = get_transitions(pre_func_co_names, new_co_names)
    transitions_co_varnames = get_transitions(pre_func_co_varnames, new_co_varnames)

    load_const_transitions = {
        OpCode.LOAD_CONST + get_bytecode(key): OpCode.LOAD_CONST + get_bytecode(value)
        for key, value in transitions_co_consts.items()
    }

    load_global_transitions = {
        OpCode.LOAD_GLOBAL + get_bytecode(key): OpCode.LOAD_GLOBAL + get_bytecode(value)
        for key, value in transitions_co_names.items()
    }

    load_fast_transitions = {
        OpCode.LOAD_FAST + get_bytecode(key): OpCode.LOAD_FAST + get_bytecode(value)
        for key, value in transitions_co_varnames.items()
    }

    store_fast_transitions = {
        OpCode.STORE_FAST + get_bytecode(key): OpCode.STORE_FAST + get_bytecode(value)
        for key, value in transitions_co_varnames.items()
    }

    pre_func_co_code = multiple_replace(pre_func_co_code, load_const_transitions)
    pre_func_co_code = multiple_replace(pre_func_co_code, load_global_transitions)
    pre_func_co_code = multiple_replace(pre_func_co_code, load_fast_transitions)
    pre_func_co_code = multiple_replace(pre_func_co_code, store_fast_transitions)

    new_co_code = pre_func_co_code[:-4] + func_co_code

    return new_co_code, new_co_consts, new_co_names, new_co_varnames


def inline(pre_func: FunctionType, func: FunctionType):
    """Insert `prefunc` at the beginning of `func` and return the corresponding
    function.

    `pre_func` should not have a return statement (else a ValueError is raised).
    `pre_func` should not have any argument (else a TypeError is raised)

    This approach takes less CPU instructions than the standard decorator approach.

    Example:

    def pre_func():
        a = "bonjour"
        print(a)

    def func(x, y):
        z = x + 2 * y
        return z ** 2

    The returned function corresponds to:

    def inlined(x, y):
        a = "bonjour"
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

    new_attributes = get_new_func_attributes(pre_func, func)
    new_co_code, new_co_consts, new_co_names, new_co_varnames = new_attributes

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
