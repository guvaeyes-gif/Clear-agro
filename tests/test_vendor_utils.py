import pandas as pd

from src.vendor_utils import build_vendor_selector_options, canonical_vendor_name, normalize_vendor_identity


def test_canonical_vendor_name_prefers_id_mapping():
    vendor_map = pd.DataFrame(
        [
            {"vendedor_id": "123", "vendedor": "Ana"},
            {"vendedor_id": "456", "vendedor": "Bruno"},
        ]
    )

    assert canonical_vendor_name("Ana (123)", vendor_map) == "Ana"
    assert canonical_vendor_name("456", vendor_map) == "Bruno"


def test_canonical_vendor_name_keeps_unmapped_numeric_label():
    vendor_map = pd.DataFrame([{"vendedor_id": "123", "vendedor": "Ana"}])

    assert canonical_vendor_name("789", vendor_map) == ""


def test_build_vendor_selector_options_dedupes_by_name():
    vendor_map = pd.DataFrame(
        [
            {"vendedor_id": "123", "vendedor": "Ana"},
            {"vendedor_id": "456", "vendedor": "Bruno"},
        ]
    )

    options = build_vendor_selector_options(
        {
            "Ana (123)": 100,
            "123": 50,
            "Bruno (456)": 25,
            "Bruno": 10,
        },
        {"Ana (123)", "123", "Bruno (456)", "Bruno"},
        vendor_map,
    )

    assert options == ["TODOS", "Ana", "Bruno"]


def test_build_vendor_selector_options_keeps_unmapped_numeric_labels():
    options = build_vendor_selector_options({}, {"789"}, pd.DataFrame())

    assert options == ["TODOS"]


def test_normalize_vendor_identity_fills_name_and_missing_id():
    vendor_map = pd.DataFrame(
        [
            {"vendedor_id": "123", "vendedor": "Ana"},
            {"vendedor_id": "456", "vendedor": "Bruno"},
        ]
    )
    df = pd.DataFrame(
        [
            {"vendedor_id": "", "vendedor": "Ana", "meta_valor": 10},
            {"vendedor_id": "456", "vendedor": "", "meta_valor": 20},
        ]
    )

    out = normalize_vendor_identity(df, vendor_map)

    assert out.loc[0, "vendedor_id"] == "123"
    assert out.loc[0, "vendedor"] == "Ana"
    assert out.loc[1, "vendedor_id"] == "456"
    assert out.loc[1, "vendedor"] == "Bruno"
