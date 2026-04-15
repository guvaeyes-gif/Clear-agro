@echo off
setlocal
cd /d "%~dp0"
python importar_metas_supabase.py --input "%~dp0metas_comerciais_template.csv"
echo.
pause
