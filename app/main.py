import streamlit as st
import pandas as pd
import altair as alt
from io import BytesIO
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

BASE = ROOT / "out" / "base_unificada.xlsx"

from src.data import load_sheets, load_bling_realizado, load_bling_nfe
from src.metrics import compute_kpis, vendedor_performance_period, meta_realizado_mensal, sparkline_last_months, period_label
from src.viz import fmt_brl_abbrev, fmt_brl, fmt_pct, bar_meta_realizado, bar_meta_realizado_single, sparkline
from src.metas_db import init_db, list_metas, create_meta, update_meta, pause_metas, summary_targets, transfer_assets, transfer_metas_futuras, seed_demo
from src.telegram import build_alerts_message, send_telegram_message, telegram_enabled

APP_TITLE = "McKinsey Agro CRM"
DEFAULT_YEAR = 2026

st.set_page_config(page_title=APP_TITLE, layout="wide")

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1280px; margin: 0 auto;}
    h1, h2, h3 {color:#1f2a44;}
    div[data-testid="stMetricValue"] {font-size: 20px; white-space: nowrap;}
    div[data-testid="stMetricLabel"] {font-size: 12px;}
    </style>
    """,
    unsafe_allow_html=True,
)


def style_table(df: pd.DataFrame, numeric_cols=None):
    if numeric_cols is None:
        numeric_cols = []
    styler = df.style
    styler = styler.apply(lambda x: ["background-color: #f7f8fa" if i % 2 else "" for i in range(len(x))], axis=0)
    styler = styler.set_properties(**{"text-align": "left"})
    if numeric_cols:
        cols = [c for c in numeric_cols if c in df.columns]
        if cols:
            styler = styler.set_properties(subset=cols, **{"text-align": "right"})
    return styler


def status_chip(s: str) -> str:
    s = (s or "").upper()
    if s == "ATIVO":
        return "ATIVO"
    if s == "PAUSADO":
        return "PAUSADO"
    if s == "DESLIGADO":
        return "DESLIGADO"
    if s == "TRANSFERIDO":
        return "TRANSFERIDO"
    return s or "-"


init_db()

if st.sidebar.button("Recarregar base"):
    try:
        load_sheets.clear()
    except Exception:
        pass
sheets = load_sheets()
if not sheets:
    st.warning("Base nao encontrada em ./out/base_unificada.xlsx")
    st.stop()

# Sidebar controls
years = set()
for key in ["metas", "realizado"]:
    df = sheets.get(key, pd.DataFrame())
    if not df.empty and "data" in df.columns:
        years.update(df["data"].dt.year.dropna().astype(int).unique().tolist())
years = sorted(years) if years else [DEFAULT_YEAR]

year = st.sidebar.selectbox("Ano", options=years, index=years.index(DEFAULT_YEAR) if DEFAULT_YEAR in years else 0)

months = list(range(1, 13))
month_labels = ["TODOS"] + [pd.Timestamp(year=year, month=m, day=1).strftime("%b").title() for m in months]
month_map = {"TODOS": None}
for m, label in zip(months, month_labels[1:]):
    month_map[label] = m

month_label = st.sidebar.selectbox("Mes", options=month_labels, index=0)
ytd = st.sidebar.checkbox("Ver YTD", value=True)

vendors = ["TODOS"]
if "metas" in sheets and not sheets["metas"].empty and "vendedor" in sheets["metas"].columns:
    vendors += sorted(sheets["metas"]["vendedor"].dropna().unique().tolist())
sel_vendor = st.sidebar.selectbox("Vendedor", options=vendors, index=0)

page = st.sidebar.selectbox(
    "Pagina",
    options=[
        "Executive Cockpit",
        "Pipeline Manager",
        "Performance & Ritmo",
        "Insights & Alertas",
        "Metas Comerciais",
        "Auditoria",
    ],
)

use_bling = st.sidebar.checkbox("Usar realizado do Bling", value=False)
if use_bling:
    br = load_bling_realizado()
    if not br.empty:
        sheets["realizado"] = br
    else:
        st.sidebar.info("Bling: cache nao encontrado. Usando realizado local.")

st.title(APP_TITLE)
period = period_label(year, month_map[month_label], ytd)
st.caption(f"Periodo: {period}")

# Apply vendor filter to metas/realizado
if sel_vendor != "TODOS":
    if "metas" in sheets and "vendedor" in sheets["metas"].columns:
        sheets["metas"] = sheets["metas"][sheets["metas"]["vendedor"] == sel_vendor]
    if "realizado" in sheets and "vendedor" in sheets["realizado"].columns:
        sheets["realizado"] = sheets["realizado"][sheets["realizado"]["vendedor"] == sel_vendor]

# Page A - Executive Cockpit
if page == "Executive Cockpit":
    st.subheader("Executive Cockpit")
    kpis = compute_kpis(sheets, year, month_map[month_label], ytd)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Realizado", fmt_brl_abbrev(kpis.realizado))
    c2.metric("Meta", fmt_brl_abbrev(kpis.meta))
    c3.metric("Atingimento %", fmt_pct(kpis.atingimento_pct))
    c4.metric("Gap (R$)", fmt_brl_abbrev(kpis.gap))
    c5.metric("Pipeline Ponderado", fmt_brl_abbrev(kpis.pipeline_ponderado) if kpis.pipeline_ponderado is not None else "-")
    c6.metric("% c/ Proximo Passo", fmt_pct(kpis.pct_proximo_passo) if kpis.pct_proximo_passo is not None else "-")

    series = meta_realizado_mensal(sheets, year)
    if not series.empty:
        st.subheader("Meta vs Realizado")
        if month_map[month_label] is None or ytd:
            st.altair_chart(bar_meta_realizado(series), width="stretch")
        else:
            single = series[series["data"].dt.month == month_map[month_label]].copy()
            if not single.empty:
                st.altair_chart(bar_meta_realizado_single(single), width="stretch")
                last6 = sparkline_last_months(series, 6)
                if len(last6) >= 3:
                    st.caption("Ultimos 6 meses (realizado)")
                    sp = sparkline(last6)
                    st.altair_chart(sp, width="stretch")
                else:
                    total = last6["receita"].sum() if "receita" in last6.columns else 0
                    avg = last6["receita"].mean() if "receita" in last6.columns else 0
                    best = last6["receita"].max() if "receita" in last6.columns else 0
                    st.info(f"Ultimos 6 meses: Realizado total {fmt_brl_abbrev(total)} | Media {fmt_brl_abbrev(avg)} | Melhor mes {fmt_brl_abbrev(best)}")
            else:
                st.info("Pendencia: sem dados no mes selecionado.")
    else:
        st.info("Pendencia: faltam dados de metas ou realizado com data.")

    # So what
    st.subheader("So what?")
    bullets = []
    perf = vendedor_performance_period(sheets, year, month_map[month_label], ytd)
    if not perf.empty:
        perf = perf.sort_values("gap", ascending=False)
        top_gap = perf.head(3)
        bullets.append("Top gaps vs meta: " + ", ".join(top_gap["vendedor"].tolist()))

        zero_real = perf[(perf["receita"] == 0) & (perf["meta"] > 0)]
        if not zero_real.empty:
            bullets.append("0 realizado (com meta): " + ", ".join(zero_real["vendedor"].head(5).tolist()))

        total = perf["receita"].sum()
        top5 = perf.sort_values("receita", ascending=False).head(5)
        if total > 0:
            share = top5["receita"].sum() / total * 100
            bullets.append(f"Concentracao top 5: {share:.0f}% do realizado")

    # Mes em risco
    if month_map[month_label] is not None and not ytd:
        today = pd.Timestamp.today()
        if today.year == year and today.month == month_map[month_label]:
            esperado = (today.day / today.days_in_month) * kpis.meta
            if kpis.meta > 0 and kpis.realizado < esperado * 0.8:
                bullets.append("Mes em risco: realizado abaixo do esperado")

    # Disciplina
    opps = sheets.get("oportunidades", pd.DataFrame())
    if "data_proximo_passo" in opps.columns:
        sem_passo = opps[opps["data_proximo_passo"].isna()]
        bullets.append(f"Disciplina: {len(sem_passo)} oportunidades sem proximo passo")
    else:
        bullets.append("Pendencia: coluna data_proximo_passo ausente")

    while len(bullets) < 5:
        bullets.append("Pendencia: dados insuficientes para este insight")
    for b in bullets[:5]:
        st.write(f"- {b}")

# Page B - Pipeline Manager
if page == "Pipeline Manager":
    st.subheader("Pipeline Manager")
    opps = sheets.get("oportunidades", pd.DataFrame())
    if opps.empty:
        st.info("Pendencia: aba oportunidades vazia")
    else:
        df = opps.copy()
        if "volume_potencial" in df.columns:
            df["valor"] = df["volume_potencial"]
        if "probabilidade" in df.columns:
            df["prob"] = pd.to_numeric(df["probabilidade"], errors="coerce")
        if "data_proximo_passo" in df.columns:
            df["proximo_passo"] = df["data_proximo_passo"]
        df["oportunidade"] = df.get("oportunidade", df.get("cliente", ""))

        if "canal" in df.columns:
            canal = st.sidebar.multiselect("Canal", sorted(df["canal"].dropna().unique().tolist()))
            if canal:
                df = df[df["canal"].isin(canal)]
        if "etapa" in df.columns:
            etapa = st.sidebar.multiselect("Etapa", sorted(df["etapa"].dropna().unique().tolist()))
            if etapa:
                df = df[df["etapa"].isin(etapa)]

        df["alerta"] = ""
        if "proximo_passo" in df.columns:
            df.loc[df["proximo_passo"].isna(), "alerta"] = "Sem proximo passo"
        if "valor" in df.columns and "prob" in df.columns:
            df["score"] = df["valor"] * (df["prob"].fillna(0) / 100)
        elif "valor" in df.columns:
            df["score"] = df["valor"]
        else:
            df["score"] = None

        cols = ["cliente", "oportunidade", "etapa", "valor", "prob", "proximo_passo", "alerta", "score", "vendedor"]
        view = df[[c for c in cols if c in df.columns]].copy()
        st.dataframe(
            view,
            height=420,
        )

        out = BytesIO()
        view.to_excel(out, index=False, sheet_name="prioridades")
        st.download_button("Exportar Prioridades da Semana", data=out.getvalue(), file_name="prioridades_semana.xlsx")

# Page C - Performance & Ritmo
if page == "Performance & Ritmo":
    st.subheader("Performance & Ritmo")
    perf = vendedor_performance_period(sheets, year, month_map[month_label], ytd)
    if perf.empty:
        st.info("Pendencia: metas/realizado por vendedor nao disponivel")
    else:
        perf = perf.sort_values("gap", ascending=False)
        perf["rank"] = range(1, len(perf) + 1)
        perf_disp = perf.copy()
        perf_disp["meta"] = perf_disp["meta"].apply(fmt_brl_abbrev)
        perf_disp["receita"] = perf_disp["receita"].apply(fmt_brl_abbrev)
        perf_disp["gap"] = perf_disp["gap"].apply(fmt_brl_abbrev)
        perf_disp["atingimento_pct"] = perf_disp["atingimento_pct"].apply(fmt_pct)
        perf_disp = perf_disp[["vendedor", "meta", "receita", "atingimento_pct", "gap", "rank"]]
        topn = st.slider("Top N", min_value=5, max_value=30, value=15)
        st.dataframe(
            perf_disp.head(topn),
            height=420,
            column_config={
                "vendedor": st.column_config.TextColumn(width="large"),
                "meta": st.column_config.TextColumn(width="small"),
                "receita": st.column_config.TextColumn(width="small"),
                "atingimento_pct": st.column_config.TextColumn(width="small"),
                "gap": st.column_config.TextColumn(width="small"),
                "rank": st.column_config.NumberColumn(width="small"),
            },
        )

    acts = sheets.get("atividades", pd.DataFrame())
    if acts.empty:
        st.info("Pendencia: sem dados de atividades")

# Page D - Insights & Alertas
if page == "Insights & Alertas":
    st.subheader("Insights & Alertas")
    opps = sheets.get("oportunidades", pd.DataFrame())
    alerts = []

    if "data_proximo_passo" in opps.columns:
        sem_passo = opps[opps["data_proximo_passo"].isna()]
        alerts.append(("Sem proximo passo", len(sem_passo)))
    else:
        alerts.append(("Sem proximo passo", "pendente"))

    for title, val in alerts:
        st.metric(title, val)

# Page F - Auditoria
if page == "Auditoria":
    st.subheader("Auditoria: Planilha vs Bling (NFe)")
    st.write("Comparativo mensal entre realizado da planilha e faturamento NFe do Bling.")

    # Planilha (realizado)
    real = sheets.get("realizado", pd.DataFrame()).copy()
    if "data" in real.columns:
        real["data"] = pd.to_datetime(real["data"], errors="coerce")
    if "receita" in real.columns:
        real["receita"] = pd.to_numeric(real["receita"], errors="coerce")
    real = real.dropna(subset=["data"])
    real = real[real["data"].dt.year == year]
    real_m = real.groupby(real["data"].dt.to_period("M"))["receita"].sum().reset_index()
    real_m["data"] = real_m["data"].dt.to_timestamp()

    # Bling NFe
    nfe = load_bling_nfe(year)
    if nfe.empty:
        st.warning("Cache NFe não encontrado para o ano selecionado. Gere nfe_2026_cache.jsonl.")
        st.stop()
    nfe = nfe.copy()
    nfe = nfe[nfe["data"].dt.year == year]
    nfe_m = nfe.groupby(nfe["data"].dt.to_period("M"))["valor"].sum().reset_index()
    nfe_m["data"] = nfe_m["data"].dt.to_timestamp()

    # Merge
    df = pd.merge(real_m, nfe_m, on="data", how="outer").fillna(0)
    df["delta"] = df["receita"] - df["valor"]
    df["delta_pct"] = df.apply(lambda r: (r["delta"] / r["valor"] * 100) if r["valor"] else 0, axis=1)
    df = df.sort_values("data")
    df["mes"] = df["data"].dt.strftime("%b/%Y").str.upper()

    st.bar_chart(df.set_index("mes")[["receita", "valor"]])
    st.dataframe(
        df[["mes", "receita", "valor", "delta", "delta_pct"]],
        height=420,
    )

    st.divider()
    st.caption("Telegram (opcional): configure TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID para enviar alertas.")
    if st.button("Enviar alertas para Telegram", key="send_telegram_alerts"):
        if not telegram_enabled():
            st.error("Telegram nao configurado. Defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID no ambiente.")
        else:
            msg = build_alerts_message(APP_TITLE + " - Insights", period, alerts)
            ok, detail = send_telegram_message(msg)
            if ok:
                st.success(detail)
            else:
                st.error(detail)

# Page E - Metas Comerciais
if page == "Metas Comerciais":
    st.subheader("Metas Comerciais")
    tabs = st.tabs(["Executive Summary", "Metas", "Cadastro", "Transferencia"])

    with tabs[0]:
        st.write("Resumo executivo das metas por UF, vendedor e periodo.")
        colf1, colf2, colf3, colf4, colf5 = st.columns(5)
        periodo_tipo = colf1.selectbox("Periodo", ["MONTH", "QUARTER"], key="metas_periodo_tipo")
        # dynamic UF list
        all_metas = list_metas({"ano": year})
        uf_opts = [""] + sorted(all_metas["estado"].dropna().unique().tolist()) if not all_metas.empty else [""]
        uf = colf2.selectbox("UF (opcional)", options=uf_opts, key="metas_uf")
        # dynamic vendedor list
        vend_opts = [""] + sorted(all_metas["vendedor_id"].dropna().unique().tolist()) if not all_metas.empty else [""]
        vend = colf3.selectbox("Vendedor ID (opcional)", options=vend_opts, key="metas_vendedor")
        status = colf4.multiselect("Status", ["ATIVO","PAUSADO","DESLIGADO","TRANSFERIDO"], key="metas_status")
        if colf5.button("Criar dados demo", key="metas_seed"):
            seed_demo()
            st.success("Dados demo criados.")

        # periodo filter
        if periodo_tipo == "MONTH":
            mes = st.selectbox("Mes", [""] + list(range(1, 13)), key="metas_mes")
            quarter = None
        else:
            quarter = st.selectbox("Quarter", [""] + [1, 2, 3, 4], key="metas_quarter")
            mes = None

        filtros = {
            "ano": year,
            "periodo_tipo": periodo_tipo,
            "mes": mes or None,
            "quarter": quarter or None,
            "estado": uf or None,
            "vendedor_id": vend or None,
            "status": status or None,
        }
        res = summary_targets(filtros)
        k = res["kpis"]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Meta", fmt_brl_abbrev(k.get("meta", 0)))
        c2.metric("Realizado", fmt_brl_abbrev(k.get("realizado", 0)))
        c3.metric("Atingimento %", fmt_pct(k.get("atingimento_pct", 0)))
        c4.metric("Delta", fmt_brl_abbrev(k.get("delta", 0)))

        if not res["series"].empty:
            ser = res["series"].rename(columns={"meta_valor":"meta","realizado_valor":"receita"}).copy()
            if periodo_tipo == "QUARTER":
                if "quarter" in ser.columns:
                    ser["periodo"] = ser["quarter"]
                else:
                    ser["periodo"] = ser["mes"].apply(lambda m: ((int(m) - 1) // 3 + 1) if pd.notna(m) else None)
            else:
                ser["periodo"] = ser["mes"]
            line = alt.Chart(ser).transform_fold(
                ["meta","receita"], as_=["tipo","valor"]
            ).mark_line(point=True).encode(
                x=alt.X("periodo:O", title="Periodo"),
                y=alt.Y("valor:Q", title="Valor"),
                color=alt.Color("tipo:N", title=""),
                tooltip=["periodo:O","tipo:N","valor:Q"],
            )
            st.altair_chart(line, width="stretch")

        # Barras por UF
        if not res["uf"].empty:
            uf_df = res["uf"].copy()
            uf_df["ating"] = (uf_df["realizado_valor"] / uf_df["meta_valor"] * 100).fillna(0)
            st.write("Atingimento por UF")
            st.bar_chart(uf_df.set_index("estado")[["meta_valor","realizado_valor"]])

            # Waterfall simple (delta por UF)
            uf_df["delta"] = uf_df["realizado_valor"] - uf_df["meta_valor"]
            st.write("Delta por UF")
            st.bar_chart(uf_df.set_index("estado")[["delta"]])

        # Heatmap UF x periodo
        dfm = list_metas(filtros)
        if not dfm.empty:
            if periodo_tipo == "MONTH":
                dfm["periodo"] = dfm["mes"]
            else:
                dfm["periodo"] = dfm["quarter"]
            pivot = dfm.pivot_table(index="estado", columns="periodo", values="realizado_valor", aggfunc="sum", fill_value=0)
            heat = pivot.reset_index().melt(id_vars=["estado"], var_name="periodo", value_name="realizado")
            st.write("Heatmap UF x periodo")
            hm = alt.Chart(heat).mark_rect().encode(
                x=alt.X("periodo:O", title="Periodo"),
                y=alt.Y("estado:N", title="UF"),
                color=alt.Color("realizado:Q", title="Realizado"),
                tooltip=["estado","periodo","realizado"],
            )
            st.altair_chart(hm, width="stretch")

    with tabs[1]:
        st.write("Listagem de metas")
        df = list_metas({"ano": year})
        if not df.empty and "status" in df.columns:
            df = df.copy()
            df["status"] = df["status"].apply(status_chip)
        st.dataframe(df, height=420)

    with tabs[2]:
        st.write("Cadastrar nova meta")
        step = st.radio("Etapa", ["Periodo", "Segmentacao", "Valores", "Revisao"], horizontal=True, key="meta_step")

        if "meta_form" not in st.session_state:
            st.session_state["meta_form"] = {}

        mf = st.session_state["meta_form"]

        if step == "Periodo":
            mf["periodo_tipo"] = st.selectbox("Periodo", ["MONTH", "QUARTER"], index=0, key="meta_periodo_tipo")
            mf["mes"] = st.selectbox("Mes", list(range(1, 13)), key="meta_mes") if mf["periodo_tipo"] == "MONTH" else None
            mf["quarter"] = st.selectbox("Quarter", [1,2,3,4], key="meta_quarter") if mf["periodo_tipo"] == "QUARTER" else None

        if step == "Segmentacao":
            mf["estado"] = st.text_input("UF (ex: PR, RS)", key="meta_uf")
            mf["vendedor_id"] = st.text_input("Vendedor ID", key="meta_vendedor")
            mf["canal"] = st.text_input("Canal (opcional)", key="meta_canal")
            mf["cultura"] = st.text_input("Cultura (opcional)", key="meta_cultura")

        if step == "Valores":
            mf["meta_valor"] = st.number_input("Meta (R$)", min_value=0.0, step=1000.0, key="meta_valor")
            mf["meta_volume"] = st.number_input("Meta Volume (opcional)", min_value=0.0, step=1.0, key="meta_volume")
            mf["status"] = st.selectbox("Status", ["ATIVO","PAUSADO","DESLIGADO","TRANSFERIDO"], key="meta_status")

        if step == "Revisao":
            st.write("Revise os dados antes de salvar.")
            st.json(mf)
            if st.button("Salvar"):
                # validacoes simples
                if mf.get("periodo_tipo") == "MONTH" and not mf.get("mes"):
                    st.error("Mes obrigatorio para MONTH.")
                elif mf.get("periodo_tipo") == "QUARTER" and not mf.get("quarter"):
                    st.error("Quarter obrigatorio para QUARTER.")
                elif not mf.get("estado") or len(mf.get("estado","")) != 2:
                    st.error("UF obrigatoria (2 letras).")
                elif mf.get("meta_valor") is None:
                    st.error("Meta obrigatoria.")
                else:
                    create_meta({
                        "ano": year,
                        "periodo_tipo": mf.get("periodo_tipo"),
                        "mes": mf.get("mes"),
                        "quarter": mf.get("quarter"),
                        "estado": mf.get("estado"),
                        "vendedor_id": mf.get("vendedor_id"),
                        "canal": mf.get("canal"),
                        "cultura": mf.get("cultura"),
                        "meta_valor": mf.get("meta_valor"),
                        "meta_volume": mf.get("meta_volume"),
                        "realizado_valor": None,
                        "realizado_volume": None,
                        "status": mf.get("status") or "ATIVO",
                        "observacoes": None,
                    }, actor_id="ui")
                    st.success("Meta criada.")

    with tabs[3]:
        st.write("Transferencia de ativos/metas")
        col1, col2 = st.columns(2)
        with col1:
            origem = st.text_input("Vendedor origem")
        with col2:
            destino = st.text_input("Vendedor destino")
        if st.button("Transferir ativos"):
            transfer_assets(origem, destino, actor_id="ui")
            st.success("Ativos transferidos")
        if st.button("Transferir metas futuras"):
            transfer_metas_futuras(origem, destino, actor_id="ui")
            st.success("Metas transferidas")
