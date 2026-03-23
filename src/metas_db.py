from __future__ import annotations

import json
import os
import subprocess
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


def _supabase_access_token() -> str:
    env_token = _env('SUPABASE_ACCESS_TOKEN')
    if env_token:
        return env_token
    token_path = _token_path()
    if token_path.exists():
        return token_path.read_text(encoding='utf-8').strip()
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
        with _connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute('select 1 from public.sales_targets limit 1')
        return
    if mode == 'supabase-rest':
        _rest_request('GET', 'sales_targets', params={'select': 'id', 'limit': '1'})
        return
    sqlite_legacy.init_db()


def list_metas(filters: Dict[str, Any] | None = None) -> pd.DataFrame:
    mode = _backend_mode()
    if mode == 'postgres':
        return _list_metas_pg(filters)
    if mode == 'supabase-rest':
        return _list_metas_rest(filters)
    return sqlite_legacy.list_metas(filters)


def create_meta(data: Dict[str, Any], actor_id: str = 'system') -> str | int:
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
        'source_system': 'manual',
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
                      status, source_system, notes, metadata
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    returning id::text
                    ''',
                    (
                        payload['target_year'], payload['period_type'], payload['month_num'], payload['quarter_num'],
                        payload['state'], payload['sales_rep_code'], payload['channel'], payload['crop'],
                        payload['target_value'], payload['target_volume'], payload['actual_value'], payload['actual_volume'],
                        payload['status'], payload['source_system'], payload['notes'], json.dumps(payload['metadata'])
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
