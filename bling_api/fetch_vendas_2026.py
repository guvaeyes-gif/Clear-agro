import json, os, time, requests

TOKEN_FILE = r"C:\Users\cesar.zarovski\bling_api\bling_tokens.json"
CACHE = r"C:\Users\cesar.zarovski\bling_api\vendas_2026_cache.jsonl"

with open(TOKEN_FILE, "r", encoding="utf-8") as f:
    tokens = json.load(f)
access_token = tokens.get("access_token")
if not access_token:
    raise SystemExit("access_token not found")

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    "enable-jwt": "1",
}

start_date = "2026-01-01"
end_date = "2026-12-31"

base_url = "https://api.bling.com.br/Api/v3/pedidos/vendas"
page = 1
limit = 100

seen = set()
if os.path.exists(CACHE):
    with open(CACHE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                if obj.get("id"):
                    seen.add(obj.get("id"))
            except:
                pass

new_count = 0
while True:
    params = {
        "pagina": page,
        "limite": limit,
        "dataEmissaoInicial": start_date,
        "dataEmissaoFinal": end_date,
    }
    resp = requests.get(base_url, headers=headers, params=params, timeout=30)
    if resp.status_code != 200:
        raise SystemExit(f"List error {resp.status_code}: {resp.text}")
    data = resp.json().get("data", [])
    if not data:
        break
    for item in data:
        id_ = item.get("id")
        if not id_ or id_ in seen:
            continue
        with open(CACHE, "a", encoding="utf-8") as f:
            f.write(json.dumps(item) + "\n")
        seen.add(id_)
        new_count += 1
    page += 1
    time.sleep(0.2)

print(f"New fetched: {new_count}")
print(CACHE)
