@echo off
setlocal
python scripts\import_sales_targets.py --source google-sheet --default-company CZ
endlocal
