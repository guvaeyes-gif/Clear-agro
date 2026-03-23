@echo off
cd /d C:\Users\cesar.zarovski\Documents\Clear_OS
powershell -ExecutionPolicy Bypass -File "C:\Users\cesar.zarovski\Documents\Clear_OS\scripts\run_crm_stable.ps1" -Port 8510 -HostAddress 127.0.0.1 -OpenBrowser
pause

