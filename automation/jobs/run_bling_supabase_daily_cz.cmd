@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "C:\Users\cesar.zarovski\Documents\Clear_OS\integrations\bling\runners\run_bling_supabase_daily.ps1" -Company CZ -Year 2026 -FromDate 2025-01-01 >> "C:\Users\cesar.zarovski\Documents\Clear_OS\logs\integration\scheduler\task_runner_cz.log" 2>&1
