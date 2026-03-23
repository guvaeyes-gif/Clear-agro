@echo off
setlocal

cd /d C:\Users\cesar.zarovski\Documents\Clear_OS
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set RUN_TS=%%i
set RUN_ID=dashboard_%RUN_TS%

python scripts\finance_dashboard_publisher.py --config templates\default_config.yaml --run-id %RUN_ID%

echo.
echo Concluido. Verifique:
echo out\dashboard_financeiro_v1\finance_dashboard_publisher_%RUN_ID%_status.json

endlocal
