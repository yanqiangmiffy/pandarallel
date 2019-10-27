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
        return x * y

    def another_func(x, y):
        c = 4
        print(c + str(x + y))
        return x * y

    assert inliner.are_functions_equivalent(a_func, a_func)
    assert not inliner.are_functions_equivalent(a_func, another_func)


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


def test_get_new_func_attributes():
    def pre_func_which_returns(a, b):
        c = "Hello"
        print(c + " " + a + " " + b)
        return c + " " + a + " " + b

    def pre_func(a, b):
        c = "Hello"
        print(c + " " + a + " " + b)

    def func(x, y):
        z = x + 2 * y
        return z ** 2

    with pytest.raises(ValueError):
        inliner.get_new_func_attributes(pre_func_which_returns, lambda x: x, ("a", "b"))

    # new_co_code = b"d\x02}\x03t\x00|\x03\x83\x01\x01\x00" + func.__code__.co_code
    # new_co_consts = (None, 2, "bonjour")
    # new_co_names = ("print",)
    # new_co_varnames = ("x", "y", "z", "a")

    # new_results = new_co_code, new_co_consts, new_co_names, new_co_varnames

    # assert inliner.get_new_func_attributes(pre_func, func) == new_results


# def test_inline():
#     def pre_func(a, b):
#         c = "Hello"
#         print(c + " " + a + " " + b)

#     def func(x, y):
#         z = x + 2 * y
#         return z ** 2

#     inlined_func = inliner.inline(pre_func, func, ("One", "Two", "Three"))

#     assert func(3, 4) == inlined_func(3, 4)
