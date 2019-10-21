import pytest

from pandarallel.utils import inliner


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


def test_get_hex():
    with pytest.raises(ValueError):
        inliner.get_hex(-1)

    assert inliner.get_hex(3) == "03"
    assert inliner.get_hex(16) == "10"
    assert inliner.get_hex(1000) == "03e8"


def test_get_bytecode():
    with pytest.raises(ValueError):
        inliner.get_bytecode(-1)

    assert inliner.get_bytecode(0) == b"\x00"
    assert inliner.get_bytecode(5) == b"\x05"
    assert inliner.get_bytecode(10) == b"\x0A"
    assert inliner.get_bytecode(255) == b"\xFF"


def test_has_freevar():
    def decorator(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    @decorator
    def function_with_freevar():
        pass

    def function_without_freevar():
        pass

    assert inliner.has_freevar(function_with_freevar)
    assert not inliner.has_freevar(function_without_freevar)


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


def test_inline():
    def pre_func_which_returns():
        return 42

    def pre_func_with_parameter(x):
        print(x)

    with pytest.raises(ValueError):
        inliner.inline(pre_func_which_returns, lambda x: x)

    with pytest.raises(TypeError):
        inliner.inline(pre_func_with_parameter, lambda x: x)