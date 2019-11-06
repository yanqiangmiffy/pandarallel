import math
import sys

import pytest

from pandarallel.utils import inliner
from types import CodeType, FunctionType


def test_multiple_find():
    bytecode = b"\x01\x03\x04\x02\x01\x05\x01"

    assert inliner.multiple_find(bytecode, b"\x01") == (0, 4, 6)
    assert inliner.multiple_find(bytecode, b"\x10") == ()


def test_multiple_replace():
    bytecode = b"\x01\x02\x03\x01"
    rep_dict = {b"\x01": b"\x02", b"\x02": b"\x03"}

    expected_output = b"\x02\x03\x03\x02"

    assert inliner.multiple_replace(bytecode, rep_dict) == expected_output


def test_remove_duplicates():
    tuple_ = (3, 1, 2, 2, 1, 4)
    expected_output = (3, 1, 2, 4)

    assert inliner.remove_duplicates(tuple_) == expected_output


def test_get_instructions_tuples():
    def function(x, y):
        print(x, y)

    python_version = sys.version_info

    if python_version.major == 3:
        if python_version.minor == 5:
            assert tuple(inliner.get_instructions_tuples(function)) == (
                b"t\x00\x00",
                b"|\x00\x00",
                b"|\x01\x00",
                b"\x83\x02\x00",
                b"\x01",
                b"d\x00\x00",
                b"S",
            )
        elif python_version.minor in (6, 7):
            assert tuple(inliner.get_instructions_tuples(function)) == (
                b"t\x00",
                b"|\x00",
                b"|\x01",
                b"\x83\x02",
                b"\x01\x00",
                b"d\x00",
                b"S\x00",
            )
    else:
        with pytest.raises(SystemError):
            inliner.get_instructions_offsets(function)


def test_has_no_return():
    def func_return_nothing(a, b):
        if a > b:
            print(a)
        else:
            print("Hello World!")

    def func_return_something(a, b):
        print(a)
        return b

    def func_several_returns(a, b):
        if a > b:
            print(a)
            return

    assert inliner.has_no_return(func_return_nothing)
    assert not inliner.has_no_return(func_return_something)
    assert not inliner.has_no_return(func_several_returns)


def test_has_duplicates():
    assert not inliner.has_duplicates([1, 3, 2, 4])
    assert inliner.has_duplicates([1, 3, 2, 3])


def test_key2value():
    sources = ("a", "b", "d", "hello")
    sources_duplicate = ("a", "b", "d", "hello", "b")

    dests = (54, "e", 2)
    dests_duplicate = (54, "e", 2, "e")

    source2dest = {"b": 55, "hello": "world"}
    source2dest_with_extra_key = {"b": 55, "hello": "world", "toto": 4}

    expected_result = (("a", "d"), (54, "e", 2, 55, "world"), {1: 3, 3: 4})

    with pytest.raises(ValueError):
        inliner.key2value(sources_duplicate, dests, source2dest)

    with pytest.raises(ValueError):
        inliner.key2value(sources, dests_duplicate, source2dest)

    with pytest.raises(ValueError):
        inliner.key2value(sources, dests, source2dest_with_extra_key)

    assert inliner.key2value(sources, dests, source2dest) == expected_result


def test_get_transitions():
    with pytest.raises(ValueError):
        inliner.get_transitions((1, 2, 2), (1, 2, 3))

    with pytest.raises(ValueError):
        inliner.get_transitions((1, 2), (1, 2, 2))

    olds = ("a", "c", "b", "d")
    news_1 = ("f", "g", "c", "d", "b", "a")
    news_2 = ("c", "d")

    assert inliner.get_transitions(olds, news_1) == {0: 5, 1: 2, 2: 4, 3: 3}
    assert inliner.get_transitions(olds, news_2) == {1: 0, 3: 1}


def test_get_b_transitions():
    transitions = {1: 3, 2: 5, 3: 6}
    byte_source = inliner.OpCode.LOAD_CONST
    byte_dest = inliner.OpCode.STORE_FAST

    bytes_transitions = inliner.get_b_transitions(transitions, byte_source, byte_dest)

    expected = {
        (byte_source + b"\x01"): (byte_dest + b"\x03"),
        (byte_source + b"\x02"): (byte_dest + b"\x05"),
        (byte_source + b"\x03"): (byte_dest + b"\x06"),
    }

    assert bytes_transitions == expected


def test_are_functions_equivalent():
    def a_func(x, y):
        c = 3
        print(c + str(x + y))
        return x * math.sin(y)

    def another_func(x, y):
        c = 4
        print(c + str(x + math.sin(y)))
        return x * y

    assert inliner.are_functions_equivalent(a_func, a_func)
    assert not inliner.are_functions_equivalent(a_func, another_func)


def test_get_shifted_bytecode():

    # TODO: Test also JUMP_IF_TRUE_OR_POP
    def func(x, y):
        if x > y:
            pass
        else:
            pass

        if not x > y:
            pass

        0 < 0 == 0

        for i in range(42):
            pass

    python_version = sys.version_info

    if python_version.major == 3:
        if python_version.minor == 5:
            bytecode = b"".join(
                (
                    b"|\x00\x00",
                    b"|\x01\x00",
                    b"k\x04\x00",
                    # JUMP_POP_IF_FALSE
                    b"r\x0f\x00",
                    b"n\x00\x00",
                    b"|\x00\x00",
                    b"|\x01\x00",
                    b"k\x04\x00",
                    # JUMP_POP_IF_TRUE
                    b"s\x1b\x00",
                    b"d\x01\x00",
                    b"d\x01\x00",
                    b"\x04",
                    b"\x03",
                    b"k\x00\x00",
                    # JUMP_IF_FALSE_OR_POP
                    b"o2\x00",
                    b"d\x01\x00",
                    b"k\x02\x00" b"n\x02\x00",
                    b"\x02",
                    b"\x01",
                    b"\x01",
                    b"x\x14\x00",
                    b"t\x00\x00",
                    b"d\x02\x00",
                    b"\x83\x01\x00",
                    b"D",
                    b"]\x06\x00",
                    b"}\x02\x00",
                    # JUMP_ABSOLUTE
                    b"qB\x00",
                    b"W",
                    b"d\x00\x00",
                    b"S",
                )
            )

            expected_shifted_bytecode = b"".join(
                (
                    b"|\x00\x00",
                    b"|\x01\x00",
                    b"k\x04\x00",
                    # JUMP_POP_IF_FALSE
                    b"r\x11\x00",
                    b"n\x00\x00",
                    b"|\x00\x00",
                    b"|\x01\x00",
                    b"k\x04\x00",
                    # JUMP_POP_IF_TRUE
                    b"s\x1d\x00",
                    b"d\x01\x00",
                    b"d\x01\x00",
                    b"\x04",
                    b"\x03",
                    b"k\x00\x00",
                    # JUMP_IF_FALSE_OR_POP
                    b"o4\x00",
                    b"d\x01\x00",
                    b"k\x02\x00" b"n\x02\x00",
                    b"\x02",
                    b"\x01",
                    b"\x01",
                    b"x\x14\x00",
                    b"t\x00\x00",
                    b"d\x02\x00",
                    b"\x83\x01\x00",
                    b"D",
                    b"]\x06\x00",
                    b"}\x02\x00",
                    # JUMP_ABSOLUTE
                    b"qD\x00",
                    b"W",
                    b"d\x00\x00",
                    b"S",
                )
            )

            assert bytecode == func.__code__.co_code
            assert inliner.get_shifted_bytecode(func, 2) == expected_shifted_bytecode

        elif python_version.minor in (6, 7):
            pass
    else:
        with pytest.raises(SystemError):
            inliner.get_instructions_offsets(function)

    # assert inliner.get_shifted_bytecode(test_func, 2) == expected_shifted_bytecode


def test_pin_arguments():
    def func(a, b):
        c = 4
        print(str(a) + str(c))

        return b

    def expected_pinned_func():
        c = 4
        print(str(10) + str(c))

        return 11

    with pytest.raises(TypeError):
        inliner.pin_arguments(func, dict(a=1))

    with pytest.raises(TypeError):
        inliner.pin_arguments(func, dict(a=1, b=2, c=3))

    pinned_func = inliner.pin_arguments(func, dict(a=10, b=11))

    assert inliner.are_functions_equivalent(pinned_func, expected_pinned_func)


def test_inline():
    def pre_func(b, c):
        a = "hello"
        print(a + " " + b + " " + c)

    def func(x, y):
        try:
            if x > y:
                z = x + 2 * math.sin(y)
                return z ** 2
            elif x == y:
                return 4
            else:
                return 2 ** 3
        except ValueError:
            foo = 0
            for i in range(4):
                foo += i
            return foo
        except TypeError:
            return 42
        else:
            return 33
        finally:
            print("finished")

    def target_inlined_func(x, y):
        # Pinned pre_func
        a = "hello"
        print(a + " " + "pretty" + " " + "world!")

        # func
        try:
            if x > y:
                z = x + 2 * math.sin(y)
                return z ** 2
            elif x == y:
                return 4
            else:
                return 2 ** 3
        except ValueError:
            foo = 0
            for i in range(4):
                foo += i
            return foo
        except TypeError:
            return 42
        else:
            return 33
        finally:
            print("finished")

    inlined_func = inliner.inline(pre_func, func, dict(b="pretty", c="world!"))

    assert inliner.are_functions_equivalent(inlined_func, target_inlined_func)
