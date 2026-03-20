"""Tests for deterministic signal query compilation."""

import copy
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.signal_intake.signal_query_compiler import compile_signal_queries


def test_compile_signal_queries_generates_keyword_suffix_combinations():
    result = compile_signal_queries(
        keywords=["beta", "alpha"],
        suffixes=["forum", "blog"],
    )

    assert result == [
        "alpha blog",
        "alpha forum",
        "beta blog",
        "beta forum",
    ]


def test_compile_signal_queries_returns_keywords_when_suffixes_missing():
    result = compile_signal_queries(keywords=["bravo", "alpha", "alpha"])

    assert result == ["alpha", "bravo"]


def test_compile_signal_queries_returns_suffixes_when_keywords_missing():
    result = compile_signal_queries(suffixes=["zeta", "alpha", "alpha"])

    assert result == ["alpha", "zeta"]


def test_compile_signal_queries_returns_empty_list_when_inputs_are_none():
    assert compile_signal_queries() == []


def test_compile_signal_queries_rejects_invalid_keywords_type():
    try:
        compile_signal_queries(keywords="alpha")
    except TypeError as error:
        assert str(error) == "keywords must be a list or None"
    else:
        raise AssertionError("TypeError was not raised for invalid keywords")


def test_compile_signal_queries_rejects_invalid_suffixes_type():
    try:
        compile_signal_queries(suffixes="alpha")
    except TypeError as error:
        assert str(error) == "suffixes must be a list or None"
    else:
        raise AssertionError("TypeError was not raised for invalid suffixes")


def test_compile_signal_queries_rejects_empty_or_non_string_keyword_items():
    for invalid_keywords, expected_message in (
        ([1], "each keyword must be a non-empty string"),
        ([""], "each keyword must be a non-empty string"),
    ):
        try:
            compile_signal_queries(keywords=invalid_keywords)
        except (TypeError, ValueError) as error:
            assert str(error) == expected_message
        else:
            raise AssertionError("Invalid keyword item was not rejected")


def test_compile_signal_queries_rejects_empty_or_non_string_suffix_items():
    for invalid_suffixes, expected_message in (
        ([1], "each suffix must be a non-empty string"),
        ([""], "each suffix must be a non-empty string"),
    ):
        try:
            compile_signal_queries(suffixes=invalid_suffixes)
        except (TypeError, ValueError) as error:
            assert str(error) == expected_message
        else:
            raise AssertionError("Invalid suffix item was not rejected")


def test_compile_signal_queries_does_not_mutate_inputs():
    keywords = ["beta", "alpha"]
    suffixes = ["forum", "blog"]

    original_keywords = copy.deepcopy(keywords)
    original_suffixes = copy.deepcopy(suffixes)

    compile_signal_queries(keywords=keywords, suffixes=suffixes)

    assert keywords == original_keywords
    assert suffixes == original_suffixes
