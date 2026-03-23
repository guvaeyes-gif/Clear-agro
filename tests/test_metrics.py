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


def test_compute_kpis_quarter():
    sheets = {
        "realizado": pd.DataFrame(
            {
                "data": pd.to_datetime(["2026-01-05", "2026-03-20", "2026-04-01"]),
                "receita": [100.0, 250.0, 900.0],
            }
        ),
        "metas": pd.DataFrame(
            {
                "data": pd.to_datetime(["2026-01-01", "2026-02-01", "2026-03-01", "2026-04-01"]),
                "meta": [300.0, 300.0, 300.0, 999.0],
            }
        ),
        "oportunidades": pd.DataFrame(),
        "atividades": pd.DataFrame(),
    }
    kpis = compute_kpis(sheets, year=2026, month=None, ytd=False, quarter=1)
    assert kpis.realizado == 350.0
    assert kpis.meta == 900.0
    assert kpis.gap == 550.0


def test_compute_kpis_prefers_pipeline_view_when_available():
    sheets = {
        "realizado": pd.DataFrame(
            {
                "data": pd.to_datetime(["2026-01-05"]),
                "receita": [100.0],
            }
        ),
        "metas": pd.DataFrame(
            {
                "data": pd.to_datetime(["2026-01-01"]),
                "meta": [500.0],
            }
        ),
        "oportunidades": pd.DataFrame(
            {
                "volume_potencial": [9999.0],
                "probabilidade": [10.0],
                "data_proximo_passo": [pd.NaT],
            }
        ),
        "atividades": pd.DataFrame(),
    }
    pipeline_view = pd.DataFrame(
        {
            "pipeline_value": [200.0, 300.0],
            "weighted_pipeline_value": [80.0, 90.0],
            "opportunities_count": [2, 3],
            "opportunities_without_next_step": [1, 1],
        }
    )

    kpis = compute_kpis(sheets, year=2026, month=1, ytd=False, pipeline_view=pipeline_view)

    assert kpis.pipeline_total == 500.0
    assert kpis.pipeline_ponderado == 170.0
    assert kpis.pct_proximo_passo == 60.0
