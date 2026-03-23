PYTHON=python

bootstrap:
	$(PYTHON) -m pip install -r requirements.txt
	$(PYTHON) -m pip install -r requirements-dev.txt

ingest:
	$(PYTHON) src/ingest/ingest_dre.py
	$(PYTHON) src/ingest/ingest_bling.py
	$(PYTHON) src/ingest/ingest_banks.py

reports:
	$(PYTHON) src/reports/build_cfo_pack.py

finance:
	$(PYTHON) src/reports/build_finance_pack.py

finance-all: ingest reports

build:
	$(PYTHON) scripts/validate_kpis.py

dashboard:
	$(PYTHON) -m streamlit run app/main.py

lint:
	$(PYTHON) -m ruff check .

test:
	$(PYTHON) -m pytest

check-migration-governance:
	$(PYTHON) scripts/check_migration_governance.py
