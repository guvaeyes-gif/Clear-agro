@echo off
set ROOT=%~dp0
powershell -ExecutionPolicy Bypass -File "%ROOT%scripts\run_crm_stable.ps1" -Port 8510 -HostAddress 127.0.0.1 -OpenBrowser
pause

