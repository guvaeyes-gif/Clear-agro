import pandas as pd

from src.metas_db import build_quarter_rollups_from_monthly, prepare_sales_targets_import


def test_prepare_sales_targets_import_normalizes_month_rows():
    raw = pd.DataFrame(
        {
            "Ano": [2026],
            "Mes": [1],
            "Estado": ["pr"],
            "Meta": [12345.67],
            "Vendedor": ["V001"],
            "Canal": ["revenda"],
            "Cultura": ["soja"],
            "Status": ["ativo"],
        }
    )

    valid, invalid, warnings = prepare_sales_targets_import(raw, default_empresa="CR")

    assert invalid.empty
    assert warnings == []
    assert len(valid) == 1
    row = valid.iloc[0]
    assert row["ano"] == 2026
    assert row["periodo_tipo"] == "MONTH"
    assert row["mes"] == 1
    assert pd.isna(row["quarter"])
    assert row["estado"] == "PR"
    assert row["vendedor_id"] == "V001"
    assert row["empresa"] == "CR"
    assert row["meta_valor"] == 12345.67
    assert row["status"] == "ATIVO"


def test_prepare_sales_targets_import_rejects_invalid_rows():
    raw = pd.DataFrame(
        {
            "Ano": [2026],
            "Estado": [""],  # invalid on purpose
            "Meta": [-1],
        }
    )

    valid, invalid, warnings = prepare_sales_targets_import(raw, default_empresa="CZ")

    assert valid.empty
    assert warnings == []
    assert len(invalid) == 1
    assert "estado ausente ou invalido" in invalid.iloc[0]["errors"]
    assert "meta_valor nao pode ser negativo" in invalid.iloc[0]["errors"]


def test_build_quarter_rollups_from_monthly_sums_three_months():
    monthly = pd.DataFrame(
        {
            "ano": [2026, 2026, 2026],
            "periodo_tipo": ["MONTH", "MONTH", "MONTH"],
            "mes": [1, 2, 3],
            "estado": ["PR", "PR", "PR"],
            "vendedor_id": ["V001", "V001", "V001"],
            "empresa": ["CZ", "CZ", "CZ"],
            "canal": ["REVENDA", "REVENDA", "REVENDA"],
            "cultura": ["SOJA", "SOJA", "SOJA"],
            "meta_valor": [100, 200, 300],
            "meta_volume": [1, 2, 3],
            "realizado_valor": [10, 20, 30],
            "realizado_volume": [0, 0, 0],
            "status": ["ATIVO", "ATIVO", "ATIVO"],
            "observacoes": [None, None, None],
        }
    )

    quarter = build_quarter_rollups_from_monthly(monthly)

    assert len(quarter) == 1
    row = quarter.iloc[0]
    assert row["periodo_tipo"] == "QUARTER"
    assert row["quarter"] == 1
    assert row["mes"] is pd.NA or pd.isna(row["mes"])
    assert row["meta_valor"] == 600
    assert row["meta_volume"] == 6
    assert row["realizado_valor"] == 60
