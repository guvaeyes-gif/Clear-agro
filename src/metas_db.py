from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "metas.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS metas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ano INTEGER NOT NULL,
                periodo_tipo TEXT NOT NULL CHECK (periodo_tipo IN ('MONTH','QUARTER')),
                mes INTEGER CHECK (mes BETWEEN 1 AND 12),
                quarter INTEGER CHECK (quarter BETWEEN 1 AND 4),
                estado TEXT NOT NULL,
                vendedor_id TEXT,
                canal TEXT,
                cultura TEXT,
                meta_valor REAL NOT NULL CHECK (meta_valor >= 0),
                meta_volume REAL,
                realizado_valor REAL CHECK (realizado_valor >= 0),
                realizado_volume REAL,
                status TEXT NOT NULL CHECK (status IN ('ATIVO','PAUSADO','DESLIGADO','TRANSFERIDO')),
                observacoes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_meta
            ON metas(ano, periodo_tipo, mes, quarter, estado, vendedor_id, canal, cultura)
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ativos_custodia (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tipo_ativo TEXT NOT NULL CHECK (tipo_ativo IN ('CONTA','OPORTUNIDADE','CLIENTE','OUTRO')),
                referencia_id TEXT NOT NULL,
                vendedor_id_atual TEXT NOT NULL,
                vendedor_id_anterior TEXT,
                status_custodia TEXT NOT NULL CHECK (status_custodia IN ('ATIVO','EM_TRANSFERENCIA','TRANSFERIDO')),
                data_transferencia TEXT,
                motivo TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor_id TEXT NOT NULL,
                acao TEXT NOT NULL,
                entidade TEXT NOT NULL,
                entidade_id TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                timestamp TEXT NOT NULL
            )
            """
        )
        conn.commit()


def _now() -> str:
    return datetime.utcnow().isoformat()


def _audit(conn: sqlite3.Connection, actor_id: str, acao: str, entidade: str, entidade_id: str, before: Any, after: Any) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO audit_log(actor_id, acao, entidade, entidade_id, before_json, after_json, timestamp)
        VALUES(?,?,?,?,?,?,?)
        """,
        (
            actor_id,
            acao,
            entidade,
            str(entidade_id),
            json.dumps(before, ensure_ascii=False) if before is not None else None,
            json.dumps(after, ensure_ascii=False) if after is not None else None,
            _now(),
        ),
    )


def list_metas(filters: Dict[str, Any] | None = None) -> pd.DataFrame:
    filters = filters or {}
    where = []
    params: list[Any] = []
    for key, val in filters.items():
        if val is None or val == "":
            continue
        if key in {"ano","periodo_tipo","mes","quarter","estado","vendedor_id","status","canal","cultura"}:
            if isinstance(val, (list, tuple, set)):
                placeholders = ",".join(["?"] * len(val))
                where.append(f"{key} IN ({placeholders})")
                params.extend(list(val))
            else:
                where.append(f"{key} = ?")
                params.append(val)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    with _connect() as conn:
        df = pd.read_sql_query(f"SELECT * FROM metas{where_sql}", conn, params=params)
    return df


def create_meta(data: Dict[str, Any], actor_id: str = "system") -> int:
    init_db()
    payload = data.copy()
    payload["created_at"] = _now()
    payload["updated_at"] = _now()
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO metas(ano, periodo_tipo, mes, quarter, estado, vendedor_id, canal, cultura,
                              meta_valor, meta_volume, realizado_valor, realizado_volume, status, observacoes,
                              created_at, updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                payload["ano"],
                payload["periodo_tipo"],
                payload.get("mes"),
                payload.get("quarter"),
                payload["estado"],
                payload.get("vendedor_id"),
                payload.get("canal"),
                payload.get("cultura"),
                payload["meta_valor"],
                payload.get("meta_volume"),
                payload.get("realizado_valor"),
                payload.get("realizado_volume"),
                payload["status"],
                payload.get("observacoes"),
                payload["created_at"],
                payload["updated_at"],
            ),
        )
        new_id = cur.lastrowid
        _audit(conn, actor_id, "CREATE", "metas", str(new_id), None, payload)
        conn.commit()
    return int(new_id)


def update_meta(meta_id: int, updates: Dict[str, Any], actor_id: str = "system") -> None:
    init_db()
    with _connect() as conn:
        before = conn.execute("SELECT * FROM metas WHERE id = ?", (meta_id,)).fetchone()
        if not before:
            return
        updates = updates.copy()
        updates["updated_at"] = _now()
        set_sql = ",".join([f"{k} = ?" for k in updates.keys()])
        params = list(updates.values()) + [meta_id]
        conn.execute(f"UPDATE metas SET {set_sql} WHERE id = ?", params)
        after = conn.execute("SELECT * FROM metas WHERE id = ?", (meta_id,)).fetchone()
        _audit(conn, actor_id, "UPDATE", "metas", str(meta_id), dict(before), dict(after))
        conn.commit()


def pause_metas(meta_ids: Iterable[int], status: str, actor_id: str = "system") -> None:
    init_db()
    with _connect() as conn:
        for mid in meta_ids:
            before = conn.execute("SELECT * FROM metas WHERE id = ?", (mid,)).fetchone()
            if not before:
                continue
            conn.execute("UPDATE metas SET status = ?, updated_at = ? WHERE id = ?", (status, _now(), mid))
            after = conn.execute("SELECT * FROM metas WHERE id = ?", (mid,)).fetchone()
            _audit(conn, actor_id, "STATUS_CHANGE", "metas", str(mid), dict(before), dict(after))
        conn.commit()


def summary_targets(filters: Dict[str, Any]) -> Dict[str, Any]:
    # if quarter requested, derive from MONTH data
    filters = filters.copy()
    is_quarter = filters.get("periodo_tipo") == "QUARTER"
    quarter_filter = filters.get("quarter")
    if is_quarter:
        filters["periodo_tipo"] = "MONTH"
        filters.pop("quarter", None)
    df = list_metas(filters)
    if df.empty:
        return {"kpis": {}, "series": pd.DataFrame(), "uf": pd.DataFrame(), "vendedor": pd.DataFrame()}
    df["realizado_valor"] = pd.to_numeric(df["realizado_valor"], errors="coerce").fillna(0)
    df["meta_valor"] = pd.to_numeric(df["meta_valor"], errors="coerce").fillna(0)
    kpis = {
        "meta": float(df["meta_valor"].sum()),
        "realizado": float(df["realizado_valor"].sum()),
    }
    kpis["atingimento_pct"] = (kpis["realizado"] / kpis["meta"] * 100) if kpis["meta"] else 0.0
    kpis["delta"] = kpis["realizado"] - kpis["meta"]

    # series
    if "quarter" not in df.columns or df["quarter"].isna().all():
        df["quarter"] = df["mes"].apply(lambda m: ((int(m) - 1) // 3 + 1) if pd.notna(m) else None)

    if is_quarter:
        if quarter_filter:
            df = df[df["quarter"] == int(quarter_filter)]
        series = df.groupby(["ano", "quarter"])[["meta_valor", "realizado_valor"]].sum().reset_index()
    else:
        series = df.groupby(["ano", "mes"])[["meta_valor", "realizado_valor"]].sum().reset_index()

    # breakdowns
    by_uf = df.groupby("estado")[["meta_valor", "realizado_valor"]].sum().reset_index()
    by_vend = df.groupby("vendedor_id")[["meta_valor", "realizado_valor"]].sum().reset_index()

    return {"kpis": kpis, "series": series, "uf": by_uf, "vendedor": by_vend}


def desligar_vendedor(vendedor_id: str, actor_id: str = "system") -> None:
    init_db()
    with _connect() as conn:
        metas = conn.execute(
            "SELECT * FROM metas WHERE vendedor_id = ? AND status = 'ATIVO' AND (mes IS NULL OR mes >= ?)",
            (vendedor_id, datetime.utcnow().month),
        ).fetchall()
        for m in metas:
            conn.execute("UPDATE metas SET status = 'DESLIGADO', updated_at = ? WHERE id = ?", (_now(), m["id"]))
            after = conn.execute("SELECT * FROM metas WHERE id = ?", (m["id"],)).fetchone()
            _audit(conn, actor_id, "DESLIGAR_VENDEDOR", "metas", str(m["id"]), dict(m), dict(after))
        conn.commit()


def transfer_assets(vendedor_origem: str, vendedor_destino: str, actor_id: str = "system") -> None:
    init_db()
    with _connect() as conn:
        ativos = conn.execute("SELECT * FROM ativos_custodia WHERE vendedor_id_atual = ?", (vendedor_origem,)).fetchall()
        for a in ativos:
            conn.execute(
                """
                UPDATE ativos_custodia
                SET vendedor_id_anterior = ?, vendedor_id_atual = ?, status_custodia = 'TRANSFERIDO',
                    data_transferencia = ?, updated_at = ?
                WHERE id = ?
                """,
                (vendedor_origem, vendedor_destino, _now(), _now(), a["id"]),
            )
            after = conn.execute("SELECT * FROM ativos_custodia WHERE id = ?", (a["id"],)).fetchone()
            _audit(conn, actor_id, "TRANSFERIR_ATIVO", "ativos_custodia", str(a["id"]), dict(a), dict(after))
        conn.commit()


def transfer_metas_futuras(vendedor_origem: str, vendedor_destino: str, actor_id: str = "system") -> None:
    init_db()
    with _connect() as conn:
        metas = conn.execute(
            "SELECT * FROM metas WHERE vendedor_id = ? AND status = 'ATIVO'", (vendedor_origem,)
        ).fetchall()
        for m in metas:
            conn.execute(
                """
                UPDATE metas
                SET vendedor_id = ?, status = 'TRANSFERIDO', updated_at = ?
                WHERE id = ?
                """,
                (vendedor_destino, _now(), m["id"]),
            )
            after = conn.execute("SELECT * FROM metas WHERE id = ?", (m["id"],)).fetchone()
            _audit(conn, actor_id, "TRANSFERIR_META", "metas", str(m["id"]), dict(m), dict(after))
        conn.commit()


def seed_demo() -> None:
    init_db()
    # only seed if empty
    if not list_metas().empty:
        return
    demo = [
        {"ano": 2026, "periodo_tipo": "MONTH", "mes": 1, "estado": "PR", "vendedor_id": "V001", "meta_valor": 120000, "realizado_valor": 90000, "status": "ATIVO"},
        {"ano": 2026, "periodo_tipo": "MONTH", "mes": 1, "estado": "RS", "vendedor_id": "V002", "meta_valor": 110000, "realizado_valor": 70000, "status": "ATIVO"},
        {"ano": 2026, "periodo_tipo": "MONTH", "mes": 2, "estado": "PR", "vendedor_id": "V001", "meta_valor": 130000, "realizado_valor": 60000, "status": "ATIVO"},
        {"ano": 2026, "periodo_tipo": "QUARTER", "quarter": 1, "estado": "PR", "vendedor_id": "V001", "meta_valor": 360000, "realizado_valor": 150000, "status": "ATIVO"},
        {"ano": 2026, "periodo_tipo": "QUARTER", "quarter": 1, "estado": "RS", "vendedor_id": "V002", "meta_valor": 330000, "realizado_valor": 120000, "status": "ATIVO"},
    ]
    for d in demo:
        d.update({"canal": None, "cultura": None, "meta_volume": None, "realizado_volume": None, "observacoes": None})
        create_meta(d, actor_id="seed")
