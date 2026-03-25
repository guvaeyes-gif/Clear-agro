@echo off
set ROOT=%~dp0
python -m streamlit run "%ROOT%app\main.py" --server.port 8502 --server.address 127.0.0.1
pause
