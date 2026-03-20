"""Tests for deterministic signal source catalog construction."""

import copy
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.signal_intake.signal_source_catalog import (
    build_signal_source_catalog,
)


def test_build_signal_source_catalog_sorts_sources_deterministically():
    result = build_signal_source_catalog(
        [
            {
                "source_id": "src_b",
                "source_label": "Bravo",
                "base_url": "https://bravo.example",
                "channel_type": "web",
                "is_active": True,
            },
            {
                "source_id": "src_a",
                "source_label": "Alpha",
                "base_url": "https://alpha.example",
                "channel_type": "forum",
                "is_active": False,
            },
        ]
    )

    assert result == [
        {
            "source_id": "src_a",
            "source_label": "Alpha",
            "base_url": "https://alpha.example",
            "channel_type": "forum",
            "is_active": False,
        },
        {
            "source_id": "src_b",
            "source_label": "Bravo",
            "base_url": "https://bravo.example",
            "channel_type": "web",
            "is_active": True,
        },
    ]


def test_build_signal_source_catalog_rejects_non_list_input():
    try:
        build_signal_source_catalog("invalid")
    except TypeError as error:
        assert str(error) == "signal_sources must be a list"
    else:
        raise AssertionError("TypeError was not raised for invalid source input")


def test_build_signal_source_catalog_rejects_non_dictionary_entries():
    try:
        build_signal_source_catalog(["invalid"])
    except TypeError as error:
        assert str(error) == "each signal source must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for invalid source entry")


def test_build_signal_source_catalog_rejects_missing_required_fields():
    try:
        build_signal_source_catalog(
            [
                {
                    "source_id": "src_a",
                    "source_label": "Alpha",
                    "base_url": "https://alpha.example",
                    "is_active": True,
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "signal source is missing required fields: channel_type"
    else:
        raise AssertionError("ValueError was not raised for missing source fields")


def test_build_signal_source_catalog_rejects_invalid_field_types():
    try:
        build_signal_source_catalog(
            [
                {
                    "source_id": "src_a",
                    "source_label": "Alpha",
                    "base_url": "https://alpha.example",
                    "channel_type": "forum",
                    "is_active": "yes",
                }
            ]
        )
    except TypeError as error:
        assert str(error) == "is_active must be a boolean"
    else:
        raise AssertionError("TypeError was not raised for invalid field type")


def test_build_signal_source_catalog_rejects_empty_required_string_fields():
    try:
        build_signal_source_catalog(
            [
                {
                    "source_id": "",
                    "source_label": "Alpha",
                    "base_url": "https://alpha.example",
                    "channel_type": "forum",
                    "is_active": True,
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "source_id must be a non-empty string"
    else:
        raise AssertionError("ValueError was not raised for empty required string")


def test_build_signal_source_catalog_rejects_unknown_fields():
    try:
        build_signal_source_catalog(
            [
                {
                    "source_id": "src_a",
                    "source_label": "Alpha",
                    "base_url": "https://alpha.example",
                    "channel_type": "forum",
                    "is_active": True,
                    "extra": "value",
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "signal source contains unknown fields: extra"
    else:
        raise AssertionError("ValueError was not raised for unknown source fields")


def test_build_signal_source_catalog_rejects_duplicate_source_ids():
    try:
        build_signal_source_catalog(
            [
                {
                    "source_id": "src_a",
                    "source_label": "Alpha",
                    "base_url": "https://alpha.example",
                    "channel_type": "forum",
                    "is_active": True,
                },
                {
                    "source_id": "src_a",
                    "source_label": "Beta",
                    "base_url": "https://beta.example",
                    "channel_type": "web",
                    "is_active": True,
                },
            ]
        )
    except ValueError as error:
        assert str(error) == "duplicate source_id detected: src_a"
    else:
        raise AssertionError("ValueError was not raised for duplicate source_id")


def test_build_signal_source_catalog_does_not_mutate_inputs():
    signal_sources = [
        {
            "source_id": "src_b",
            "source_label": "Bravo",
            "base_url": "https://bravo.example",
            "channel_type": "web",
            "is_active": True,
        },
        {
            "source_id": "src_a",
            "source_label": "Alpha",
            "base_url": "https://alpha.example",
            "channel_type": "forum",
            "is_active": False,
        },
    ]

    original_signal_sources = copy.deepcopy(signal_sources)

    build_signal_source_catalog(signal_sources)

    assert signal_sources == original_signal_sources
