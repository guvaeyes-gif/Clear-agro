from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SUPABASE_MIGRATIONS = ROOT / 'supabase' / 'migrations'
DATABASE_MIGRATIONS = ROOT / 'database' / 'migrations'


def main() -> int:
    errors: list[str] = []

    supabase_sql = sorted(SUPABASE_MIGRATIONS.glob('*.sql'))
    database_sql = sorted(DATABASE_MIGRATIONS.glob('*.sql'))

    if not SUPABASE_MIGRATIONS.exists():
        errors.append('Missing canonical directory: supabase/migrations')
    elif not supabase_sql:
        errors.append('No executable SQL migrations found in supabase/migrations')

    if not DATABASE_MIGRATIONS.exists():
        errors.append('Missing governance directory: database/migrations')
    elif database_sql:
        errors.append(
            'Deprecated SQL files found in database/migrations: '
            + ', '.join(path.name for path in database_sql)
        )

    if errors:
        print('Migration governance check: FAIL')
        for error in errors:
            print(f'- {error}')
        return 1

    print('Migration governance check: PASS')
    print(f'- Canonical SQL migrations: {len(supabase_sql)} file(s) in supabase/migrations')
    print('- database/migrations contains documentation only')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
