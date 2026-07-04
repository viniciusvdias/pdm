"""Tests for fraction label helpers."""

from preprocessing.fractions import fraction_label, parse_fraction_label


def test_fraction_label_integers():
    assert fraction_label(100) == "100"
    assert fraction_label(1) == "1"


def test_fraction_label_decimal():
    assert fraction_label(0.1) == "0p1"
    assert parse_fraction_label("0p1") == 0.1
