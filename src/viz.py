from __future__ import annotations

import altair as alt
import pandas as pd


def moeda_curta(x: float) -> str:
    try:
        x = float(x)
    except Exception:
        return "-"
    abs_x = abs(x)
    if abs_x >= 1_000_000_000:
        return f"R$ {x/1_000_000_000:.1f}B".replace(".", ",")
    if abs_x >= 1_000_000:
        return f"R$ {x/1_000_000:.1f}M".replace(".", ",")
    if abs_x >= 1_000:
        return f"R$ {x/1_000:.1f}K".replace(".", ",")
    s = f"{x:,.0f}"
    return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_brl_abbrev(x: float) -> str:
    return moeda_curta(x)


def pct_curto(x: float) -> str:
    try:
        x = float(x)
    except Exception:
        return "-"
    return f"{x:.0f}%"


def fmt_brl(x: float) -> str:
    return moeda_curta(x)


def fmt_pct(x: float) -> str:
    return pct_curto(x)


def bar_meta_realizado(df: pd.DataFrame) -> alt.Chart:
    tmp = df.copy()
    tmp = tmp.sort_values("data")
    tmp["mes_num"] = tmp["data"].dt.month
    tmp["mes"] = tmp["data"].dt.strftime("%b").str.title()
    tmp["meta_fmt"] = tmp["meta"].apply(moeda_curta)
    tmp["receita_fmt"] = tmp["receita"].apply(moeda_curta)
    tmp["gap_fmt"] = tmp["gap"].apply(moeda_curta)
    chart = alt.Chart(tmp).transform_fold(
        ["meta", "receita"], as_=["tipo", "valor"]
    ).mark_bar(size=20).encode(
        x=alt.X("mes:N", title="Mes", sort=alt.SortField(field="mes_num", order="ascending")),
        y=alt.Y("valor:Q", title="Valor", axis=alt.Axis(format="~s")),
        color=alt.Color("tipo:N", title="", legend=alt.Legend(orient="top")),
        tooltip=[
            alt.Tooltip("mes:N", title="Mes"),
            alt.Tooltip("meta_fmt:N", title="Meta"),
            alt.Tooltip("receita_fmt:N", title="Realizado"),
            alt.Tooltip("gap_fmt:N", title="Gap"),
        ],
    )
    return chart


def bar_meta_realizado_single(df: pd.DataFrame) -> alt.Chart:
    tmp = df.copy()
    tmp["mes"] = tmp["data"].dt.strftime("%b/%Y").str.upper()
    tmp["meta_fmt"] = tmp["meta"].apply(moeda_curta)
    tmp["receita_fmt"] = tmp["receita"].apply(moeda_curta)
    tmp["gap_fmt"] = tmp["gap"].apply(moeda_curta)
    chart = alt.Chart(tmp).transform_fold(
        ["meta", "receita"], as_=["tipo", "valor"]
    ).mark_bar(size=28).encode(
        y=alt.Y("tipo:N", title=""),
        x=alt.X("valor:Q", title="Valor", axis=alt.Axis(format="~s")),
        color=alt.Color("tipo:N", title="", legend=alt.Legend(orient="top")),
        tooltip=[
            alt.Tooltip("mes:N", title="Mes"),
            alt.Tooltip("meta_fmt:N", title="Meta"),
            alt.Tooltip("receita_fmt:N", title="Realizado"),
            alt.Tooltip("gap_fmt:N", title="Gap"),
        ],
    )
    return chart


def sparkline(df: pd.DataFrame) -> alt.Chart:
    tmp = df.copy()
    tmp["mes"] = tmp["data"].dt.strftime("%b").str.title()
    chart = alt.Chart(tmp).mark_line(point=True).encode(
        x=alt.X("mes:N", title=""),
        y=alt.Y("receita:Q", title="Realizado"),
        tooltip=[alt.Tooltip("mes:N"), alt.Tooltip("receita:Q")],
    )
    return chart
