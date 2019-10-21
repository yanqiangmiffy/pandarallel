from inspect import signature
import re
from typing import Any, Callable, Dict, Tuple


class OpCode:
    LOAD_CONST = b"d"
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

def get_transition(old: Tuple[Any, ...], new: Tuple[Any, ...]) -> Dict[int, int]:
    """Returns a dictionnary where keys


def inline(pre_func: FunctionType, func: FunctionType) -> FunctionType:
    """Insert `prefunc` at the beginning of `func` and returns a new function.

    `prefunc` should not have a return statement (else a ValueError is raised).
    `prefunc` should not have any argument (else a TypeError is raised)

    This approach takes less CPU instructions than the standard decorator approach.

    Example:

    def prefunc():
        a = "bonjour"
        print(a)

    def func(x, y):
        z = x + 2 * y
        return z ** 2

    The function returned by inline is:

    def inlined(x, y):
        a = "bonjour"
        print(a)
        z = x + 2 * y
        return z ** 2
    """

    if not has_no_return(pre_func):
        raise ValueError("`pre_func` returns something")

    if len(signature(pre_func).parameters) != 0:
        raise TypeError("`pre_func` has paramenters")

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


# def copy_func(func, name=None):
#     new_func = FunctionType(
#         func.__code__,
#         func.__globals__,
#         name or func.__name__,
#         func.__defaults__,
#         func.__closure__,
#     )

#     # In case f was given attrs (note this dict is a shallow copy):
#     new_func.__dict__.update(func.__dict__)

#     return new_func


# def replace_load_fast_by_load_const(bytecode, varname_index2const_index):
#     varname_index2const_index = {
#         b"|" + c_uint8(fast_index): b"d" + c_uint8(const_index)
#         for fast_index, const_index in varname_index2const_index.items()
#     }

#     return replace(bytecode, varname_index2const_index)


# def replace_fast_by_fast(bytecode, varname_index2varname_new_index):
#     # STORE_FAST
#     store_varname_index2varname_new_index = {
#         b"}" + c_uint8(fast_index): b"}" + c_uint8(fast_new_index)
#         for fast_index, fast_new_index in varname_index2varname_new_index.items()
#     }

#     bytecode = replace(bytecode, store_varname_index2varname_new_index)

#     # LOAD_FAST
#     load_varname_index2varname_new_index = {
#         b"|" + c_uint8(fast_index): b"|" + c_uint8(fast_new_index)
#         for fast_index, fast_new_index in varname_index2varname_new_index.items()
#     }

#     bytecode = replace(bytecode, load_varname_index2varname_new_index)

#     return bytecode


# def inlined_partial(func, name, **arg_name2value):
#     # TODO: This function does not work if all the arguments of the source
#     #       function are not pinned. (Probably because arguments of the dest
#     #       function are not located at the beginning of co_varnames)
#     #       Anyway for Pandarallel use case we will live with it.

#     for arg_name in arg_name2value:
#         if arg_name not in func.__code__.co_varnames:
#             raise KeyError(arg_name + " is not an argument of " + str(func))

#     fcode = func.__code__
#     new_consts = tuple_remove_duplicate(
#         fcode.co_consts + tuple(arg_name2value.values())
#     )
#     varname_index2new_const_index = {
#         fcode.co_varnames.index(arg_name): new_consts.index(value)
#         for arg_name, value in arg_name2value.items()
#     }

#     new_varnames = tuple(set(fcode.co_varnames) - set(arg_name2value.keys()))
#     varname_index2varname_new_index = {
#         fcode.co_varnames.index(arg_name): new_varnames.index(arg_name)
#         for arg_name in new_varnames
#     }

#     new_co_code = replace_load_fast_by_load_const(
#         fcode.co_code, varname_index2new_const_index
#     )

#     new_co_code = replace_fast_by_fast(new_co_code, varname_index2varname_new_index)

#     new_func = copy_func(func, name)

#     nfcode = new_func.__code__
#     new_func.__code__ = CodeType(
#         nfcode.co_argcount - len(arg_name2value),
#         nfcode.co_kwonlyargcount,
#         len(new_varnames),
#         nfcode.co_stacksize,
#         nfcode.co_flags,
#         new_co_code,
#         new_consts,
#         nfcode.co_names,
#         new_varnames,
#         nfcode.co_filename,
#         name,
#         nfcode.co_firstlineno,
#         nfcode.co_lnotab,
#         nfcode.co_freevars,
#         nfcode.co_cellvars,
#     )

#     return new_func
