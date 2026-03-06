import pandas as pd

from src.metrics import compute_kpis


def test_compute_kpis_month():
    sheets = {
        "realizado": pd.DataFrame(
            {
                "data": pd.to_datetime(["2026-01-05", "2026-02-01"]),
                "receita": [100.0, 200.0],
            }
        ),
        "metas": pd.DataFrame(
            {
                "data": pd.to_datetime(["2026-01-01", "2026-02-01"]),
                "meta": [500.0, 500.0],
            }
        ),
        "oportunidades": pd.DataFrame(),
        "atividades": pd.DataFrame(),
    }
    kpis = compute_kpis(sheets, year=2026, month=1, ytd=False)
    assert kpis.realizado == 100.0
    assert kpis.meta == 500.0
    assert kpis.atingimento_pct == 20.0


def test_compute_kpis_ytd():
    sheets = {
        "realizado": pd.DataFrame(
            {
                "data": pd.to_datetime(["2026-01-05", "2026-02-01"]),
                "receita": [100.0, 200.0],
            }
        ),
        "metas": pd.DataFrame(
            {
                "data": pd.to_datetime(["2026-01-01", "2026-02-01"]),
                "meta": [500.0, 500.0],
            }
        ),
        "oportunidades": pd.DataFrame(),
        "atividades": pd.DataFrame(),
    }
    kpis = compute_kpis(sheets, year=2026, month=None, ytd=True)
    assert kpis.realizado >= 100.0
    assert kpis.meta >= 500.0
