@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "C:\Users\admin\Desktop\Clear-agro\integrations\bling\runners\run_bling_supabase_daily.ps1" -Company CR -Year 2026 -FromDate 2025-01-01 >> "C:\Users\admin\Desktop\Clear-agro\logs\integration\scheduler\task_runner_cr.log" 2>&1
