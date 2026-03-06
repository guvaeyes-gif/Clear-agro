from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import threading
import os
import subprocess
import sys
from pathlib import Path

OUT = r"C:\Users\cesar.zarovski\Documents\code.txt"
ROOT = Path(__file__).resolve().parent
EXCHANGE = str(ROOT / "token_exchange.py")
PYTHON = sys.executable

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        qs = parse_qs(urlparse(self.path).query)
        code = qs.get('code', [''])[0]
        if code:
            with open(OUT, 'w', encoding='utf-8') as f:
                f.write(code)
            # exchange immediately
            try:
                p = subprocess.run(
                    [PYTHON, EXCHANGE, "--code", code],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                if p.returncode == 0:
                    msg = "OK. Code capturado e tokens gerados."
                else:
                    msg = "Falha ao trocar code. Verifique a janela do PowerShell."
                    sys.stdout.write((p.stdout or "") + "\n" + (p.stderr or "") + "\n")
            except Exception as e:
                msg = f"Falha ao trocar code: {e}"
            self.send_response(200)
            self.send_header('Content-Type','text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(("<h3>%s</h3>" % msg).encode("utf-8"))
            # shutdown server
            threading.Thread(target=self.server.shutdown, daemon=True).start()
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing code")

    def log_message(self, format, *args):
        return

if __name__ == '__main__':
    httpd = HTTPServer(('localhost', 8080), Handler)
    print('Listening on http://localhost:8080/bling/callback')
    httpd.serve_forever()
