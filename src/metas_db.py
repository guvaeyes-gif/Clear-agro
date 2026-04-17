from __future__ import annotations

import json
import os
import subprocess
import warnings
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable
from urllib.parse import urlsplit, urlunsplit

import pandas as pd

from src import metas_db_sqlite_legacy as sqlite_legacy

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover
    psycopg = None
    dict_row = None

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

ROOT = Path(__file__).resolve().parents[1]
POOLER_URL_PATH = ROOT / 'supabase' / '.temp' / 'pooler-url'
PROJECT_REF_PATH = ROOT / 'supabase' / '.temp' / 'project-ref'
DEFAULT_TOKEN_PATH = Path.home() / 'Documents' / 'token supabase.txt'

STATUS_TO_DB = {
    'ATIVO': 'active',
    'PAUSADO': 'paused',
    'DESLIGADO': 'disabled',
    'TRANSFERIDO': 'transferred',
}
STATUS_FROM_DB = {value: key for key, value in STATUS_TO_DB.items()}
PERIOD_TO_DB = {'MONTH': 'month', 'QUARTER': 'quarter'}
PERIOD_FROM_DB = {value: key for key, value in PERIOD_TO_DB.items()}
CUSTODY_TYPE_TO_DB = {'CONTA': 'account', 'OPORTUNIDADE': 'opportunity', 'CLIENTE': 'customer', 'OUTRO': 'other'}
TARGET_IMPORT_COLUMNS = [
    'ano',
    'periodo_tipo',
    'mes',
    'quarter',
    'estado',
    'vendedor_id',
    'empresa',
    'canal',
    'cultura',
    'meta_valor',
    'meta_volume',
    'realizado_valor',
    'realizado_volume',
    'status',
    'observacoes',
]
TARGET_IMPORT_STATUSES = {'ATIVO', 'PAUSADO', 'DESLIGADO', 'TRANSFERIDO'}
TARGET_IMPORT_ALIASES = {
    'year': 'ano',
    'target_year': 'ano',
    'period_type': 'periodo_tipo',
    'periodo': 'periodo_tipo',
    'period': 'periodo_tipo',
    'month': 'mes',
    'month_num': 'mes',
    'quarter_num': 'quarter',
    'trimestre': 'quarter',
    'uf': 'estado',
    'state': 'estado',
    'sales_rep_code': 'vendedor_id',
    'sales_rep_id': 'vendedor_id',
    'vendedor': 'vendedor_id',
    'company': 'empresa',
    'channel': 'canal',
    'crop': 'cultura',
    'target_value': 'meta_valor',
    'meta': 'meta_valor',
    'value': 'meta_valor',
    'target_volume': 'meta_volume',
    'actual_value': 'realizado_valor',
    'realizado': 'realizado_valor',
    'actual_volume': 'realizado_volume',
    'notes': 'observacoes',
    'observacao': 'observacoes',
    'observacoes': 'observacoes',
}


def _env(name: str) -> str:
    return (os.getenv(name) or '').strip()


def _clean_value(value: Any) -> Any:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _inject_password(url: str, password: str) -> str:
    parsed = urlsplit(url)
    if not parsed.username or parsed.password:
        return url
    username = parsed.username
    host = parsed.hostname or ''
    port = f':{parsed.port}' if parsed.port else ''
    netloc = f'{username}:{password}@{host}{port}'
    return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


def _normalize_db_url(url: str) -> str:
    if not url:
        return url
    parsed = urlsplit(url)
    query = parsed.query or ''
    if 'sslmode=' not in query and parsed.hostname not in {'localhost', '127.0.0.1'}:
        query = f'{query}&sslmode=require' if query else 'sslmode=require'
        url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))
    return url


def _database_url() -> str:
    for key in ('CRM_DATABASE_URL', 'DATABASE_URL', 'SUPABASE_DB_URL'):
        value = _env(key)
        if value:
            return _normalize_db_url(value)
    password = _env('SUPABASE_DB_PASSWORD')
    if password and POOLER_URL_PATH.exists():
        pooler_url = POOLER_URL_PATH.read_text(encoding='utf-8').strip()
        if pooler_url:
            return _normalize_db_url(_inject_password(pooler_url, password))
    return ''


def _project_ref() -> str:
    value = _env('SUPABASE_PROJECT_REF')
    if value:
        return value
    if PROJECT_REF_PATH.exists():
        return PROJECT_REF_PATH.read_text(encoding='utf-8').strip()
    return ''


def _token_path() -> Path:
    return Path(_env('SUPABASE_TOKEN_PATH') or DEFAULT_TOKEN_PATH)


def _is_valid_supabase_access_token(token: str) -> bool:
    token = (token or '').strip()
    return token.startswith('sbp_')


def _supabase_access_token() -> str:
    env_token = _env('SUPABASE_ACCESS_TOKEN')
    if _is_valid_supabase_access_token(env_token):
        return env_token
    token_path = _token_path()
    if token_path.exists():
        file_token = token_path.read_text(encoding='utf-8').strip()
        if _is_valid_supabase_access_token(file_token):
            return file_token
    return ''


def _use_postgres() -> bool:
    return bool(psycopg is not None and _database_url())


def _use_supabase_rest() -> bool:
    return bool(requests is not None and _project_ref() and _supabase_access_token())


def _backend_mode() -> str:
    if _use_postgres():
        return 'postgres'
    if _use_supabase_rest():
        return 'supabase-rest'
    return 'sqlite'


def _connect_pg():
    if psycopg is None:
        raise RuntimeError('psycopg not installed')
    db_url = _database_url()
    if not db_url:
        raise RuntimeError('DATABASE_URL/CRM_DATABASE_URL/SUPABASE_DB_URL not configured')
    return psycopg.connect(db_url, row_factory=dict_row)


@lru_cache(maxsize=1)
def _service_role_key() -> str:
    token = _supabase_access_token()
    project_ref = _project_ref()
    if not token or not project_ref:
        raise RuntimeError('Supabase token/project ref not configured')
    env = dict(os.environ)
    env['SUPABASE_ACCESS_TOKEN'] = token
    cmd = ['npx.cmd', 'supabase', 'projects', 'api-keys', '--project-ref', project_ref, '-o', 'json']
    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or 'Failed to fetch Supabase API keys')
    payload = json.loads(proc.stdout)
    if not isinstance(payload, list):
        raise RuntimeError('Unexpected Supabase API keys payload')
    for row in payload:
        if not isinstance(row, dict):
            continue
        key = str(row.get('api_key') or '').strip()
        if not key or key.startswith('sb_secret_') and key.endswith('...'):
            continue
        role_hint = str((row.get('secret_jwt_template') or {}).get('role') or '').strip().lower()
        name_hint = str(row.get('name') or '').strip().lower()
        desc_hint = str(row.get('description') or '').strip().lower()
        if 'service_role' in {role_hint, name_hint} or 'service_role' in desc_hint:
            return key
    raise RuntimeError('Service role key not found')


def _rest_headers() -> dict[str, str]:
    key = _service_role_key()
    return {
        'apikey': key,
        'Authorization': f'Bearer {key}',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    }


def _rest_url(table_or_view: str) -> str:
    return f"https://{_project_ref()}.supabase.co/rest/v1/{table_or_view}"


def _rest_request(method: str, table_or_view: str, params: dict[str, Any] | None = None, payload: Any = None, prefer: str | None = None):
    if requests is None:
        raise RuntimeError('requests not installed')
    headers = _rest_headers()
    if prefer:
        headers['Prefer'] = prefer
    response = requests.request(
        method=method,
        url=_rest_url(table_or_view),
        headers=headers,
        params=params,
        json=payload,
        timeout=60,
    )
    response.raise_for_status()
    if not response.text:
        return None
    return response.json()


def _fetch_dataframe(cur, query: str, params: list[Any]) -> pd.DataFrame:
    cur.execute(query, params)
    rows = cur.fetchall()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _map_status_to_db(status: str | None) -> str:
    return STATUS_TO_DB.get((status or '').strip().upper(), 'active')


def _map_status_from_db(status: str | None) -> str:
    return STATUS_FROM_DB.get((status or '').strip().lower(), (status or '').upper())


def _map_period_to_db(period: str | None) -> str:
    return PERIOD_TO_DB.get((period or '').strip().upper(), 'month')


def _map_period_from_db(period: str | None) -> str:
    return PERIOD_FROM_DB.get((period or '').strip().lower(), (period or '').upper())


def _prepare_metas_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if 'periodo_tipo' in out.columns:
        out['periodo_tipo'] = out['periodo_tipo'].map(_map_period_from_db)
    if 'status' in out.columns:
        out['status'] = out['status'].map(_map_status_from_db)
    return out


def _normalize_import_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(col).replace('\ufeff', '').strip().lower().replace(' ', '_') for col in out.columns]
    rename: dict[str, str] = {}
    for source, target in TARGET_IMPORT_ALIASES.items():
        if source in out.columns and target not in out.columns:
            rename[source] = target
    if rename:
        out = out.rename(columns=rename)
    return out


def _safe_text(value: Any) -> str:
    return str(_clean_value(value) or '').strip()


def prepare_sales_targets_import(
    df: pd.DataFrame,
    default_empresa: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    warnings: list[str] = []
    if df.empty:
        return pd.DataFrame(columns=TARGET_IMPORT_COLUMNS), pd.DataFrame(columns=['row_number', 'errors']), warnings

    out = _normalize_import_columns(df)
    if 'data' in out.columns:
        out['data'] = pd.to_datetime(out['data'], errors='coerce')

    if 'ano' in out.columns:
        out['ano'] = pd.to_numeric(out['ano'], errors='coerce')
    elif 'data' in out.columns:
        out['ano'] = out['data'].dt.year

    if 'mes' in out.columns:
        out['mes'] = pd.to_numeric(out['mes'], errors='coerce')
    elif 'data' in out.columns:
        out['mes'] = out['data'].dt.month
    else:
        out['mes'] = pd.NA

    if 'quarter' in out.columns:
        out['quarter'] = pd.to_numeric(out['quarter'], errors='coerce')
    elif 'data' in out.columns:
        out['quarter'] = ((out['data'].dt.month - 1) // 3 + 1)
    else:
        out['quarter'] = pd.NA

    for column in ['meta_valor', 'meta_volume', 'realizado_valor', 'realizado_volume']:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors='coerce')

    if 'status' in out.columns:
        out['status'] = out['status'].fillna('').astype(str).str.strip().str.upper()
    else:
        out['status'] = 'ATIVO'
    out['status'] = out['status'].replace('', 'ATIVO')

    if 'periodo_tipo' in out.columns:
        out['periodo_tipo'] = out['periodo_tipo'].fillna('').astype(str).str.strip().str.upper()
    else:
        out['periodo_tipo'] = ''
    out['periodo_tipo'] = out['periodo_tipo'].replace({'MONTHLY': 'MONTH', 'TRIMESTRE': 'QUARTER'})
    inferred_month = out['mes'].notna()
    inferred_quarter = out['quarter'].notna()
    out.loc[out['periodo_tipo'].eq('') & inferred_quarter, 'periodo_tipo'] = 'QUARTER'
    out.loc[out['periodo_tipo'].eq('') & inferred_month & ~inferred_quarter, 'periodo_tipo'] = 'MONTH'
    out['periodo_tipo'] = out['periodo_tipo'].replace('', 'MONTH')

    if 'empresa' in out.columns:
        out['empresa'] = out['empresa'].fillna('').astype(str).str.strip().str.upper()
    else:
        out['empresa'] = ''
    if default_empresa:
        default_empresa_txt = str(default_empresa).strip().upper()
        if default_empresa_txt and default_empresa_txt != 'AUTO':
            out.loc[out['empresa'].eq(''), 'empresa'] = default_empresa_txt
    if out['empresa'].eq('').any():
        out.loc[out['empresa'].eq(''), 'empresa'] = 'CZ'
        warnings.append('Algumas linhas estavam sem empresa; foi aplicado o fallback CZ.')

    for column in ['estado', 'canal', 'cultura', 'vendedor_id']:
        if column in out.columns:
            out[column] = out[column].fillna('').astype(str).str.strip()
    out['estado'] = out.get('estado', pd.Series('', index=out.index)).fillna('').astype(str).str.strip().str.upper()
    out['canal'] = out.get('canal', pd.Series('', index=out.index)).fillna('').astype(str).str.strip().str.upper()
    out['cultura'] = out.get('cultura', pd.Series('', index=out.index)).fillna('').astype(str).str.strip().str.upper()
    out['vendedor_id'] = out.get('vendedor_id', pd.Series('', index=out.index)).fillna('').astype(str).str.strip()
    out['observacoes'] = out.get('observacoes', pd.Series('', index=out.index)).fillna('').astype(str).str.strip()

    valid_rows: list[dict[str, Any]] = []
    invalid_rows: list[dict[str, Any]] = []

    for idx, row in out.iterrows():
        errors: list[str] = []
        ano = row.get('ano')
        estado = _safe_text(row.get('estado')).upper()
        periodo_tipo = _safe_text(row.get('periodo_tipo')).upper() or 'MONTH'
        mes = row.get('mes')
        quarter = row.get('quarter')
        meta_valor = row.get('meta_valor')

        if pd.isna(ano):
            errors.append('ano ausente')
        else:
            ano = int(ano)
            if ano < 2000:
                errors.append('ano invalido')

        if not estado or len(estado) != 2:
            errors.append('estado ausente ou invalido')

        if periodo_tipo not in {'MONTH', 'QUARTER'}:
            errors.append('periodo_tipo deve ser MONTH ou QUARTER')
        elif periodo_tipo == 'MONTH':
            if pd.isna(mes):
                errors.append('mes obrigatorio para MONTH')
            else:
                mes = int(mes)
                if mes < 1 or mes > 12:
                    errors.append('mes fora do intervalo 1-12')
            quarter = None
        else:
            if pd.isna(quarter):
                errors.append('quarter obrigatorio para QUARTER')
            else:
                quarter = int(quarter)
                if quarter < 1 or quarter > 4:
                    errors.append('quarter fora do intervalo 1-4')
            mes = None

        if pd.isna(meta_valor):
            errors.append('meta_valor obrigatorio')
        else:
            meta_valor = float(meta_valor)
            if meta_valor < 0:
                errors.append('meta_valor nao pode ser negativo')

        status = _safe_text(row.get('status')).upper() or 'ATIVO'
        if status not in TARGET_IMPORT_STATUSES:
            errors.append('status invalido')

        if errors:
            invalid_rows.append(
                {
                    'row_number': idx + 2,
                    'errors': '; '.join(errors),
                    **row.to_dict(),
                }
            )
            continue

        valid_rows.append(
            {
                'ano': int(ano),
                'periodo_tipo': periodo_tipo,
                'mes': mes,
                'quarter': quarter,
                'estado': estado,
                'vendedor_id': _safe_text(row.get('vendedor_id')),
                'empresa': _safe_text(row.get('empresa')).upper(),
                'canal': _safe_text(row.get('canal')).upper() or None,
                'cultura': _safe_text(row.get('cultura')).upper() or None,
                'meta_valor': float(meta_valor),
                'meta_volume': _clean_value(row.get('meta_volume')),
                'realizado_valor': _clean_value(row.get('realizado_valor')),
                'realizado_volume': _clean_value(row.get('realizado_volume')),
                'status': status,
                'observacoes': _safe_text(row.get('observacoes')) or None,
            }
        )

    return pd.DataFrame(valid_rows), pd.DataFrame(invalid_rows), warnings


def _spreadsheet_external_ref(row: Dict[str, Any]) -> str:
    parts = [
        str(row.get('ano', '') or ''),
        str(row.get('periodo_tipo', '') or ''),
        str(row.get('mes', '') or ''),
        str(row.get('quarter', '') or ''),
        str(row.get('estado', '') or ''),
        str(row.get('vendedor_id', '') or ''),
        str(row.get('empresa', '') or ''),
        str(row.get('canal', '') or ''),
        str(row.get('cultura', '') or ''),
    ]
    return 'spreadsheet:' + '|'.join(parts)


def build_quarter_rollups_from_monthly(valid_rows: pd.DataFrame) -> pd.DataFrame:
    if valid_rows.empty or 'periodo_tipo' not in valid_rows.columns:
        return pd.DataFrame(columns=TARGET_IMPORT_COLUMNS)
    monthly = valid_rows[valid_rows['periodo_tipo'].astype(str).str.upper() == 'MONTH'].copy()
    if monthly.empty:
        return pd.DataFrame(columns=TARGET_IMPORT_COLUMNS)

    monthly['mes'] = pd.to_numeric(monthly.get('mes'), errors='coerce')
    monthly = monthly[monthly['mes'].notna()].copy()
    if monthly.empty:
        return pd.DataFrame(columns=TARGET_IMPORT_COLUMNS)

    monthly['quarter'] = ((monthly['mes'].astype(int) - 1) // 3 + 1).astype(int)
    monthly['meta_valor'] = pd.to_numeric(monthly.get('meta_valor'), errors='coerce').fillna(0)
    for column in ['meta_volume', 'realizado_valor', 'realizado_volume']:
        if column in monthly.columns:
            monthly[column] = pd.to_numeric(monthly[column], errors='coerce').fillna(0)
        else:
            monthly[column] = 0
    monthly['empresa'] = monthly.get('empresa', pd.Series('', index=monthly.index)).fillna('').astype(str).str.upper()
    monthly['estado'] = monthly.get('estado', pd.Series('', index=monthly.index)).fillna('').astype(str).str.upper()
    monthly['vendedor_id'] = monthly.get('vendedor_id', pd.Series('', index=monthly.index)).fillna('').astype(str).str.strip()
    monthly['canal'] = monthly.get('canal', pd.Series('', index=monthly.index)).fillna('').astype(str).str.upper()
    monthly['cultura'] = monthly.get('cultura', pd.Series('', index=monthly.index)).fillna('').astype(str).str.upper()
    monthly['status'] = monthly.get('status', pd.Series('ATIVO', index=monthly.index)).fillna('ATIVO').astype(str).str.upper()

    group_cols = ['ano', 'quarter', 'estado', 'vendedor_id', 'empresa', 'canal', 'cultura']
    sums = (
        monthly.groupby(group_cols, dropna=False)[['meta_valor', 'meta_volume', 'realizado_valor', 'realizado_volume']]
        .sum()
        .reset_index()
    )
    sums['periodo_tipo'] = 'QUARTER'
    sums['mes'] = pd.NA
    sums['status'] = 'ATIVO'
    sums['observacoes'] = None
    ordered_cols = [col for col in TARGET_IMPORT_COLUMNS if col in sums.columns]
    return sums[ordered_cols]


def import_sales_targets_dataframe(
    df: pd.DataFrame,
    *,
    actor_id: str = 'system',
    default_empresa: str | None = None,
    include_quarter_rollup: bool = True,
) -> dict[str, Any]:
    valid_rows, invalid_rows, warnings = prepare_sales_targets_import(df, default_empresa=default_empresa)
    quarter_rows = build_quarter_rollups_from_monthly(valid_rows) if include_quarter_rollup else pd.DataFrame(columns=TARGET_IMPORT_COLUMNS)
    if not quarter_rows.empty:
        valid_rows = pd.concat([valid_rows, quarter_rows], ignore_index=True)
    result: dict[str, Any] = {
        'created': 0,
        'updated': 0,
        'skipped': int(len(invalid_rows)),
        'rollup_rows': int(len(quarter_rows)),
        'warnings': warnings,
        'invalid_rows': invalid_rows,
    }
    if valid_rows.empty:
        result['message'] = 'Nenhuma linha valida para importar.'
        return result

    for _, row in valid_rows.iterrows():
        payload = row.to_dict()
        payload['source_system'] = 'spreadsheet'
        payload['external_ref'] = _spreadsheet_external_ref(payload)
        update_payload = {key: value for key, value in payload.items() if key not in {'source_system', 'external_ref'}}
        filters = {
            'ano': payload['ano'],
            'periodo_tipo': payload['periodo_tipo'],
            'estado': payload['estado'],
            'empresa': payload['empresa'],
        }
        if payload.get('mes') is not None:
            filters['mes'] = payload['mes']
        if payload.get('quarter') is not None:
            filters['quarter'] = payload['quarter']
        if payload.get('vendedor_id'):
            filters['vendedor_id'] = payload['vendedor_id']
        if payload.get('canal'):
            filters['canal'] = payload['canal']
        if payload.get('cultura'):
            filters['cultura'] = payload['cultura']

        existing = list_metas(filters)
        if not existing.empty:
            match = existing.copy()
            for column in ['vendedor_id', 'canal', 'cultura']:
                value = _safe_text(payload.get(column)).upper()
                series = match.get(column, pd.Series('', index=match.index)).fillna('').astype(str).str.strip().str.upper()
                if value:
                    match = match[series.eq(value)]
                else:
                    match = match[series.eq('')]
            existing = match
        if existing.empty:
            create_meta(payload, actor_id=actor_id, source_system='spreadsheet', external_ref=payload['external_ref'])
            result['created'] += 1
        else:
            update_meta(existing.iloc[0]['id'], update_payload, actor_id=actor_id)
            result['updated'] += 1

    result['message'] = (
        f"Importação concluída: {result['created']} criadas, {result['updated']} atualizadas, "
        f"{result['skipped']} rejeitadas."
    )
    return result


def _build_rest_filters(filters: Dict[str, Any] | None = None) -> dict[str, str]:
    filters = filters or {}
    params: dict[str, str] = {
        'select': 'id,target_year,period_type,month_num,quarter_num,state,sales_rep_code,channel,crop,target_value,target_volume,actual_value,actual_volume,status,notes,created_at,updated_at',
        'limit': '5000',
        'order': 'target_year.asc,period_type.asc,month_num.asc.nullsfirst,quarter_num.asc.nullsfirst,state.asc,sales_rep_code.asc.nullslast',
    }
    simple_map = {
        'ano': 'target_year',
        'estado': 'state',
        'mes': 'month_num',
        'quarter': 'quarter_num',
        'canal': 'channel',
        'cultura': 'crop',
    }
    for key, column in simple_map.items():
        value = filters.get(key)
        if value in (None, ''):
            continue
        if isinstance(value, (list, tuple, set)):
            joined = ','.join(str(_clean_value(item)) for item in value)
            params[column] = f'in.({joined})'
        else:
            params[column] = f'eq.{_clean_value(value)}'

    period = filters.get('periodo_tipo')
    if period not in (None, ''):
        params['period_type'] = f"eq.{_map_period_to_db(period)}"

    status = filters.get('status')
    if status not in (None, ''):
        if isinstance(status, (list, tuple, set)):
            joined = ','.join(_map_status_to_db(str(item)) for item in status)
            params['status'] = f'in.({joined})'
        else:
            params['status'] = f"eq.{_map_status_to_db(str(status))}"

    vendedor = filters.get('vendedor_id')
    if vendedor not in (None, ''):
        if isinstance(vendedor, (list, tuple, set)):
            joined = ','.join(str(item) for item in vendedor)
            params['sales_rep_code'] = f'in.({joined})'
        else:
            params['sales_rep_code'] = f'eq.{vendedor}'
    return params


def _list_metas_pg(filters: Dict[str, Any] | None = None) -> pd.DataFrame:
    filters = filters or {}
    clauses: list[str] = []
    params: list[Any] = []
    mapping = {
        'ano': 'target_year',
        'estado': 'state',
        'mes': 'month_num',
        'quarter': 'quarter_num',
        'canal': 'channel',
        'cultura': 'crop',
    }
    for key, column in mapping.items():
        value = filters.get(key)
        if value in (None, ''):
            continue
        if isinstance(value, (list, tuple, set)):
            clauses.append(f'{column} = any(%s)')
            params.append(list(value))
        else:
            clauses.append(f'{column} = %s')
            params.append(value)

    if filters.get('periodo_tipo') not in (None, ''):
        clauses.append('period_type = %s')
        params.append(_map_period_to_db(filters['periodo_tipo']))
    if filters.get('status') not in (None, ''):
        status = filters['status']
        if isinstance(status, (list, tuple, set)):
            clauses.append('status = any(%s)')
            params.append([_map_status_to_db(str(item)) for item in status])
        else:
            clauses.append('status = %s')
            params.append(_map_status_to_db(str(status)))
    if filters.get('vendedor_id') not in (None, ''):
        vend = filters['vendedor_id']
        if isinstance(vend, (list, tuple, set)):
            clauses.append('sales_rep_code = any(%s)')
            params.append([str(item) for item in vend])
        else:
            clauses.append('sales_rep_code = %s')
            params.append(str(vend))

    where_sql = f"where {' and '.join(clauses)}" if clauses else ''
    query = f'''
        select
          id::text as id,
          target_year as ano,
          period_type::text as periodo_tipo,
          month_num as mes,
          quarter_num as quarter,
          state as estado,
          sales_rep_code as vendedor_id,
          channel as canal,
          crop as cultura,
          target_value as meta_valor,
          target_volume as meta_volume,
          actual_value as realizado_valor,
          actual_volume as realizado_volume,
          status::text as status,
          notes as observacoes,
          created_at,
          updated_at
        from public.sales_targets
        {where_sql}
        order by target_year, period_type, month_num nulls first, quarter_num nulls first, state, sales_rep_code nulls last
    '''
    with _connect_pg() as conn:
        with conn.cursor() as cur:
            return _prepare_metas_df(_fetch_dataframe(cur, query, params))


def _list_metas_rest(filters: Dict[str, Any] | None = None) -> pd.DataFrame:
    rows = _rest_request('GET', 'sales_targets', params=_build_rest_filters(filters)) or []
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.rename(
        columns={
            'target_year': 'ano',
            'period_type': 'periodo_tipo',
            'month_num': 'mes',
            'quarter_num': 'quarter',
            'state': 'estado',
            'sales_rep_code': 'vendedor_id',
            'channel': 'canal',
            'crop': 'cultura',
            'target_value': 'meta_valor',
            'target_volume': 'meta_volume',
            'actual_value': 'realizado_valor',
            'actual_volume': 'realizado_volume',
            'notes': 'observacoes',
        }
    )
    return _prepare_metas_df(df)


def init_db() -> None:
    mode = _backend_mode()
    if mode == 'postgres':
        try:
            with _connect_pg() as conn:
                with conn.cursor() as cur:
                    cur.execute('select 1 from public.sales_targets limit 1')
            return
        except Exception as exc:
            warnings.warn(f"Remote metas backend unavailable, falling back to sqlite: {exc}")
            sqlite_legacy.init_db()
        return
    if mode == 'supabase-rest':
        try:
            _rest_request('GET', 'sales_targets', params={'select': 'id', 'limit': '1'})
            return
        except Exception as exc:
            warnings.warn(f"Supabase REST unavailable, falling back to sqlite: {exc}")
            sqlite_legacy.init_db()
        return
    sqlite_legacy.init_db()


def list_metas(filters: Dict[str, Any] | None = None) -> pd.DataFrame:
    mode = _backend_mode()
    if mode == 'postgres':
        return _list_metas_pg(filters)
    if mode == 'supabase-rest':
        return _list_metas_rest(filters)
    return sqlite_legacy.list_metas(filters)


def create_meta(
    data: Dict[str, Any],
    actor_id: str = 'system',
    source_system: str = 'manual',
    external_ref: str | None = None,
) -> str | int:
    mode = _backend_mode()
    if mode == 'sqlite':
        return sqlite_legacy.create_meta(data, actor_id=actor_id)

    payload = {
        'target_year': int(data['ano']),
        'period_type': _map_period_to_db(data.get('periodo_tipo')),
        'month_num': _clean_value(data.get('mes')),
        'quarter_num': _clean_value(data.get('quarter')),
        'state': data['estado'],
        'sales_rep_code': _clean_value(data.get('vendedor_id')),
        'channel': _clean_value(data.get('canal')),
        'crop': _clean_value(data.get('cultura')),
        'target_value': float(_clean_value(data.get('meta_valor')) or 0),
        'target_volume': _clean_value(data.get('meta_volume')),
        'actual_value': _clean_value(data.get('realizado_valor')),
        'actual_volume': _clean_value(data.get('realizado_volume')),
        'status': _map_status_to_db(data.get('status')),
        'source_system': source_system,
        'external_ref': _clean_value(external_ref),
        'notes': _clean_value(data.get('observacoes')),
        'metadata': {'actor_id': actor_id},
    }

    if mode == 'postgres':
        with _connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    '''
                    insert into public.sales_targets (
                      target_year, period_type, month_num, quarter_num, state, sales_rep_code,
                      channel, crop, target_value, target_volume, actual_value, actual_volume,
                      status, source_system, external_ref, notes, metadata
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    returning id::text
                    ''',
                    (
                        payload['target_year'], payload['period_type'], payload['month_num'], payload['quarter_num'],
                        payload['state'], payload['sales_rep_code'], payload['channel'], payload['crop'],
                        payload['target_value'], payload['target_volume'], payload['actual_value'], payload['actual_volume'],
                        payload['status'], payload['source_system'], payload['external_ref'], payload['notes'],
                        json.dumps(payload['metadata'])
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return row['id']

    rows = _rest_request('POST', 'sales_targets', payload=payload, prefer='return=representation') or []
    return rows[0]['id'] if rows else ''


def update_meta(meta_id: int | str, updates: Dict[str, Any], actor_id: str = 'system') -> None:
    mode = _backend_mode()
    if mode == 'sqlite':
        sqlite_legacy.update_meta(meta_id, updates, actor_id=actor_id)
        return
    if not updates:
        return

    payload: dict[str, Any] = {'metadata': {'last_actor_id': actor_id}}
    mapping = {
        'ano': 'target_year',
        'mes': 'month_num',
        'quarter': 'quarter_num',
        'estado': 'state',
        'vendedor_id': 'sales_rep_code',
        'canal': 'channel',
        'cultura': 'crop',
        'meta_valor': 'target_value',
        'meta_volume': 'target_volume',
        'realizado_valor': 'actual_value',
        'realizado_volume': 'actual_volume',
        'observacoes': 'notes',
    }
    for old_key, new_key in mapping.items():
        if old_key in updates:
            payload[new_key] = _clean_value(updates[old_key])
    if 'periodo_tipo' in updates:
        payload['period_type'] = _map_period_to_db(updates['periodo_tipo'])
    if 'status' in updates:
        payload['status'] = _map_status_to_db(updates['status'])

    if mode == 'postgres':
        sets = ', '.join(f'{key} = %s' for key in payload.keys())
        params = [json.dumps(value) if key == 'metadata' else value for key, value in payload.items()]
        params.append(str(meta_id))
        with _connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(f'update public.sales_targets set {sets} where id::text = %s', params)
            conn.commit()
        return

    _rest_request('PATCH', 'sales_targets', params={'id': f'eq.{meta_id}'}, payload=payload)


def pause_metas(meta_ids: Iterable[int | str], status: str, actor_id: str = 'system') -> None:
    mode = _backend_mode()
    if mode == 'sqlite':
        sqlite_legacy.pause_metas(meta_ids, status, actor_id=actor_id)
        return
    ids = [str(item) for item in meta_ids]
    if not ids:
        return
    payload = {'status': _map_status_to_db(status), 'metadata': {'last_actor_id': actor_id}}
    if mode == 'postgres':
        with _connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'update public.sales_targets set status = %s, metadata = %s where id::text = any(%s)',
                    (payload['status'], json.dumps(payload['metadata']), ids),
                )
            conn.commit()
        return
    _rest_request('PATCH', 'sales_targets', params={'id': f"in.({','.join(ids)})"}, payload=payload)


def summary_targets(filters: Dict[str, Any]) -> Dict[str, Any]:
    if _backend_mode() == 'sqlite':
        return sqlite_legacy.summary_targets(filters)
    filters = filters.copy()
    is_quarter = filters.get('periodo_tipo') == 'QUARTER'
    quarter_filter = filters.get('quarter')
    if is_quarter:
        filters['periodo_tipo'] = 'MONTH'
        filters.pop('quarter', None)
    df = list_metas(filters)
    if df.empty:
        return {'kpis': {}, 'series': pd.DataFrame(), 'uf': pd.DataFrame(), 'vendedor': pd.DataFrame()}
    df['realizado_valor'] = pd.to_numeric(df['realizado_valor'], errors='coerce').fillna(0)
    df['meta_valor'] = pd.to_numeric(df['meta_valor'], errors='coerce').fillna(0)
    if 'quarter' not in df.columns or df['quarter'].isna().all():
        df['quarter'] = df['mes'].apply(lambda m: ((int(m) - 1) // 3 + 1) if pd.notna(m) else None)
    df_kpi = df[df['quarter'] == int(quarter_filter)] if is_quarter and quarter_filter else df
    kpis = {'meta': float(df_kpi['meta_valor'].sum()), 'realizado': float(df_kpi['realizado_valor'].sum())}
    kpis['atingimento_pct'] = (kpis['realizado'] / kpis['meta'] * 100) if kpis['meta'] else 0.0
    kpis['delta'] = kpis['realizado'] - kpis['meta']
    if is_quarter:
        if quarter_filter:
            df = df[df['quarter'] == int(quarter_filter)]
        series = df.groupby(['ano', 'quarter'])[['meta_valor', 'realizado_valor']].sum().reset_index()
    else:
        series = df.groupby(['ano', 'mes'])[['meta_valor', 'realizado_valor']].sum().reset_index()
    by_uf = df.groupby('estado')[['meta_valor', 'realizado_valor']].sum().reset_index()
    by_vend = df.groupby('vendedor_id')[['meta_valor', 'realizado_valor']].sum().reset_index()
    return {'kpis': kpis, 'series': series, 'uf': by_uf, 'vendedor': by_vend}


def desligar_vendedor(vendedor_id: str, actor_id: str = 'system') -> None:
    mode = _backend_mode()
    if mode == 'sqlite':
        sqlite_legacy.desligar_vendedor(vendedor_id, actor_id=actor_id)
        return
    payload = {'status': 'disabled', 'metadata': {'last_actor_id': actor_id}}
    if mode == 'postgres':
        with _connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'update public.sales_targets set status = %s, metadata = %s where sales_rep_code = %s and status = %s',
                    ('disabled', json.dumps(payload['metadata']), vendedor_id, 'active'),
                )
            conn.commit()
        return
    _rest_request('PATCH', 'sales_targets', params={'sales_rep_code': f'eq.{vendedor_id}', 'status': 'eq.active'}, payload=payload)


def transfer_assets(vendedor_origem: str, vendedor_destino: str, actor_id: str = 'system') -> None:
    mode = _backend_mode()
    if mode == 'sqlite':
        sqlite_legacy.transfer_assets(vendedor_origem, vendedor_destino, actor_id=actor_id)
        return
    payload = {
        'previous_sales_rep_code': vendedor_origem,
        'current_sales_rep_code': vendedor_destino,
        'custody_status': 'transferred',
        'transferred_at': pd.Timestamp.utcnow().isoformat(),
        'metadata': {'last_actor_id': actor_id, 'from': vendedor_origem, 'to': vendedor_destino},
    }
    if mode == 'postgres':
        with _connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    '''
                    update public.sales_rep_asset_custody
                    set previous_sales_rep_code = %s,
                        current_sales_rep_code = %s,
                        custody_status = %s,
                        transferred_at = %s,
                        metadata = %s
                    where current_sales_rep_code = %s
                    ''',
                    (vendedor_origem, vendedor_destino, 'transferred', payload['transferred_at'], json.dumps(payload['metadata']), vendedor_origem),
                )
            conn.commit()
        return
    _rest_request('PATCH', 'sales_rep_asset_custody', params={'current_sales_rep_code': f'eq.{vendedor_origem}'}, payload=payload)


def transfer_metas_futuras(vendedor_origem: str, vendedor_destino: str, actor_id: str = 'system') -> None:
    mode = _backend_mode()
    if mode == 'sqlite':
        sqlite_legacy.transfer_metas_futuras(vendedor_origem, vendedor_destino, actor_id=actor_id)
        return
    payload = {
        'sales_rep_code': vendedor_destino,
        'status': 'transferred',
        'metadata': {'last_actor_id': actor_id, 'from': vendedor_origem, 'to': vendedor_destino},
    }
    if mode == 'postgres':
        with _connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'update public.sales_targets set sales_rep_code = %s, status = %s, metadata = %s where sales_rep_code = %s and status = %s',
                    (vendedor_destino, 'transferred', json.dumps(payload['metadata']), vendedor_origem, 'active'),
                )
            conn.commit()
        return
    _rest_request(
        'PATCH',
        'sales_targets',
        params={'sales_rep_code': f'eq.{vendedor_origem}', 'status': 'eq.active'},
        payload=payload,
    )


def seed_demo() -> None:
    if _backend_mode() == 'sqlite':
        sqlite_legacy.seed_demo()
        return
    if not list_metas().empty:
        return
    demo = [
        {'ano': 2026, 'periodo_tipo': 'MONTH', 'mes': 1, 'estado': 'PR', 'vendedor_id': 'V001', 'meta_valor': 120000, 'realizado_valor': 90000, 'status': 'ATIVO'},
        {'ano': 2026, 'periodo_tipo': 'MONTH', 'mes': 1, 'estado': 'RS', 'vendedor_id': 'V002', 'meta_valor': 110000, 'realizado_valor': 70000, 'status': 'ATIVO'},
        {'ano': 2026, 'periodo_tipo': 'MONTH', 'mes': 2, 'estado': 'PR', 'vendedor_id': 'V001', 'meta_valor': 130000, 'realizado_valor': 60000, 'status': 'ATIVO'},
        {'ano': 2026, 'periodo_tipo': 'QUARTER', 'quarter': 1, 'estado': 'PR', 'vendedor_id': 'V001', 'meta_valor': 360000, 'realizado_valor': 150000, 'status': 'ATIVO'},
        {'ano': 2026, 'periodo_tipo': 'QUARTER', 'quarter': 1, 'estado': 'RS', 'vendedor_id': 'V002', 'meta_valor': 330000, 'realizado_valor': 120000, 'status': 'ATIVO'},
    ]
    for row in demo:
        row.update({'canal': None, 'cultura': None, 'meta_volume': None, 'realizado_volume': None, 'observacoes': None})
        create_meta(row, actor_id='seed')
