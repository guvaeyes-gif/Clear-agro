@echo off
cd /d "C:\Users\admin\Desktop\Clear-agro"
"C:\Users\admin\AppData\Local\Programs\Python\Python311\python.exe" "C:\Users\admin\Desktop\Clear-agro\scripts\publish_finance_render_snapshot.py" --push-remote fork --push-remote origin >> "C:\Users\admin\Desktop\Clear-agro\logs\integration\scheduler\finance_render_daily.log" 2>&1
