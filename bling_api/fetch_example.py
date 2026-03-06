import os
import json
import requests

TOKEN_FILE = os.path.join(os.path.dirname(__file__), "bling_tokens.json")
if not os.path.exists(TOKEN_FILE):
    raise SystemExit("Token file not found. Run token_exchange.py first.")

with open(TOKEN_FILE, "r", encoding="utf-8") as f:
    tokens = json.load(f)

access_token = tokens.get("access_token")
if not access_token:
    raise SystemExit("access_token not found in token file")

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    # Use JWT if enabled on Bling side
    "enable-jwt": "1",
}

# Example: list sales orders (pedidos de venda). Adjust endpoint as needed.
url = "https://api.bling.com.br/Api/v3/pedidos/vendas"
params = {
    "limite": 5
}

resp = requests.get(url, headers=headers, params=params, timeout=30)
print("Status:", resp.status_code)
print(resp.text)
