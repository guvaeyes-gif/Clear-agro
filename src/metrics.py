from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd


@dataclass
class KPIs:
    realizado: float
    meta: float
    atingimento_pct: float
    gap: float
    pipeline_total: float
    pipeline_ponderado: Optional[float]
    pct_proximo_passo: Optional[float]
    atividades_semana: Optional[float]


def _period_mask(df: pd.DataFrame, year: int, month: Optional[int], ytd: bool) -> pd.Series:
    if "data" not in df.columns:
        return pd.Series(True, index=df.index)
    dfy = df["data"].dt.year == year
    if ytd:
        today = pd.Timestamp.today()
        return dfy & (df["data"].dt.month <= today.month)
    if month is None:
        return dfy
    return dfy & (df["data"].dt.month == month)


def compute_kpis(sheets: Dict[str, pd.DataFrame], year: int, month: Optional[int], ytd: bool) -> KPIs:
    opps = sheets.get("oportunidades", pd.DataFrame())
    real = sheets.get("realizado", pd.DataFrame())
    metas = sheets.get("metas", pd.DataFrame())
    acts = sheets.get("atividades", pd.DataFrame())

    realizado = 0.0
    if not real.empty and "receita" in real.columns:
        mask = _period_mask(real, year, month, ytd)
        realizado = float(real.loc[mask, "receita"].fillna(0).sum())

    meta = 0.0
    if not metas.empty and "meta" in metas.columns:
        mask = _period_mask(metas, year, month, ytd)
        meta = float(metas.loc[mask, "meta"].fillna(0).sum())

    ating = (realizado / meta * 100) if meta else 0.0
    gap = meta - realizado

    pipeline_total = 0.0
    pipeline_ponderado = None
    if not opps.empty and "volume_potencial" in opps.columns:
        pipeline_total = float(opps["volume_potencial"].fillna(0).sum())
        if "probabilidade" in opps.columns:
            prob = pd.to_numeric(opps["probabilidade"], errors="coerce").fillna(0) / 100.0
            pipeline_ponderado = float((opps["volume_potencial"].fillna(0) * prob).sum())

    pct_next = None
    if "data_proximo_passo" in opps.columns:
        pct_next = float(opps["data_proximo_passo"].notna().mean() * 100)

    atividades_semana = None
    if "data" in acts.columns:
        acts = acts.copy()
        acts["data"] = pd.to_datetime(acts["data"], errors="coerce")
        last_28 = acts[acts["data"] >= (pd.Timestamp.today() - pd.Timedelta(days=28))]
        atividades_semana = float(len(last_28) / 4) if len(last_28) else 0.0

    return KPIs(
        realizado=realizado,
        meta=meta,
        atingimento_pct=ating,
        gap=gap,
        pipeline_total=pipeline_total,
        pipeline_ponderado=pipeline_ponderado,
        pct_proximo_passo=pct_next,
        atividades_semana=atividades_semana,
    )


def vendedor_performance_period(sheets: Dict[str, pd.DataFrame], year: int, month: Optional[int], ytd: bool) -> pd.DataFrame:
    metas = sheets.get("metas", pd.DataFrame())
    real = sheets.get("realizado", pd.DataFrame())

    if metas.empty or "vendedor" not in metas.columns:
        return pd.DataFrame()

    metas = metas.copy()
    metas_mask = _period_mask(metas, year, month, ytd)
    metas = metas[metas_mask]
    metas_sum = metas.groupby("vendedor")["meta"].sum().reset_index()

    real_sum = pd.DataFrame(columns=["vendedor", "receita"])
    if not real.empty and "vendedor" in real.columns:
        real_mask = _period_mask(real, year, month, ytd)
        real = real[real_mask]
        real_sum = real.groupby("vendedor")["receita"].sum().reset_index()

    df = metas_sum.merge(real_sum, on="vendedor", how="left")
    df["receita"] = df["receita"].fillna(0)
    df["atingimento_pct"] = df.apply(lambda r: (r["receita"] / r["meta"] * 100) if r["meta"] else 0.0, axis=1)
    df["gap"] = df["meta"] - df["receita"]
    return df


def meta_realizado_mensal(sheets: Dict[str, pd.DataFrame], year: int) -> pd.DataFrame:
    metas = sheets.get("metas", pd.DataFrame())
    real = sheets.get("realizado", pd.DataFrame())
    if metas.empty and real.empty:
        return pd.DataFrame()
    if "data" in metas.columns:
        metas = metas[metas["data"].dt.year == year]
        metas_m = metas.groupby(metas["data"].dt.to_period("M"))["meta"].sum().reset_index()
        metas_m["data"] = metas_m["data"].dt.to_timestamp()
    else:
        metas_m = pd.DataFrame(columns=["data", "meta"])
    if "data" in real.columns:
        real = real[real["data"].dt.year == year]
        real_m = real.groupby(real["data"].dt.to_period("M"))["receita"].sum().reset_index()
        real_m["data"] = real_m["data"].dt.to_timestamp()
    else:
        real_m = pd.DataFrame(columns=["data", "receita"])
    df = pd.merge(metas_m, real_m, on="data", how="outer").sort_values("data")
    df["meta"] = df["meta"].fillna(0)
    df["receita"] = df["receita"].fillna(0)
    df["gap"] = df["meta"] - df["receita"]
    return df


def sparkline_last_months(df: pd.DataFrame, months: int = 6) -> pd.DataFrame:
    if df.empty:
        return df
    return df.tail(months)


def period_label(year: int, month: Optional[int], ytd: bool) -> str:
    if ytd or month is None:
        return f"YTD {year}"
    return pd.Timestamp(year=year, month=month, day=1).strftime("%b/%Y").upper()
