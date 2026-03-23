from __future__ import annotations

import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src import metas_db as target_repo
from src import metas_db_sqlite_legacy as legacy_repo


def _to_payload(row: dict) -> dict:
    return {
        'target_year': int(row['ano']),
        'period_type': target_repo._map_period_to_db(row.get('periodo_tipo')),
        'month_num': target_repo._clean_value(row.get('mes')),
        'quarter_num': target_repo._clean_value(row.get('quarter')),
        'state': row['estado'],
        'sales_rep_code': target_repo._clean_value(row.get('vendedor_id')),
        'channel': target_repo._clean_value(row.get('canal')),
        'crop': target_repo._clean_value(row.get('cultura')),
        'target_value': float(target_repo._clean_value(row.get('meta_valor')) or 0),
        'target_volume': target_repo._clean_value(row.get('meta_volume')),
        'actual_value': target_repo._clean_value(row.get('realizado_valor')),
        'actual_volume': target_repo._clean_value(row.get('realizado_volume')),
        'status': target_repo._map_status_to_db(row.get('status')),
        'source_system': 'legacy_sqlite',
        'notes': target_repo._clean_value(row.get('observacoes')),
        'metadata': {'actor_id': 'legacy_migration'},
    }


def main() -> int:
    if target_repo._backend_mode() == 'sqlite':
        print('Target backend still points to sqlite; aborting migration.')
        return 2

    target_rows = target_repo.list_metas()
    if not target_rows.empty:
        print(f'Target already has {len(target_rows)} rows; skipping import.')
        return 0

    legacy_rows = legacy_repo.list_metas()
    if legacy_rows.empty:
        print('Legacy sqlite repository is empty; nothing to import.')
        return 0

    payload = [_to_payload(row) for row in legacy_rows.to_dict(orient='records')]
    batch_size = 100
    total_batches = int(math.ceil(len(payload) / batch_size))

    for idx in range(total_batches):
        start = idx * batch_size
        batch = payload[start:start + batch_size]
        target_repo._rest_request('POST', 'sales_targets', payload=batch, prefer='return=minimal')
        print(f'Inserted batch {idx + 1}/{total_batches} with {len(batch)} rows.')

    print(f'Imported {len(legacy_rows)} rows from sqlite legacy into target backend.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
