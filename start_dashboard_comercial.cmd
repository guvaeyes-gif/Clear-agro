@echo off
set "ROOT=%~dp0"
cd /d "%ROOT%"
"C:\Users\admin\AppData\Local\Programs\Python\Python311\python.exe" -m streamlit run app\main.py --server.address 127.0.0.1 --server.port 8510 --server.headless true
pause
