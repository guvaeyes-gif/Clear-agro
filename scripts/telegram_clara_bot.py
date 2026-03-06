from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.telegram import get_telegram_updates, send_telegram_message_to_chat

SYSTEM_PROMPT = (
    "Voce e Clara, secretaria executiva do usuario. "
    "Responda em portugues do Brasil, objetiva, pratica e educada. "
    "Priorize acao e proximo passo claro."
)
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_API_URL = "https://api.openai.com/v1/responses"


def _extract_text(update: dict) -> tuple[int | None, str | None]:
    msg = update.get("message") or update.get("edited_message") or {}
    chat = msg.get("chat", {})
    chat_id = chat.get("id")
    text = msg.get("text")
    if chat_id is None or not text:
        return None, None
    return int(chat_id), str(text).strip()


def _call_openai(messages: list[dict[str, str]]) -> str:
    if not OPENAI_API_KEY:
        return (
            "Conexao com IA nao configurada. Defina OPENAI_API_KEY no ambiente "
            "para eu responder por aqui."
        )

    payload = {
        "model": OPENAI_MODEL,
        "input": messages,
        "text": {"format": {"type": "text"}},
    }
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=OPENAI_API_URL,
        data=body,
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        data = json.loads(raw)
        if data.get("output_text"):
            return str(data["output_text"]).strip()

        output = data.get("output", [])
        texts: list[str] = []
        for item in output:
            for c in item.get("content", []):
                if c.get("type") == "output_text":
                    texts.append(c.get("text", ""))
        joined = "\n".join([t.strip() for t in texts if t and t.strip()]).strip()
        return joined or "Nao consegui gerar resposta agora."
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        return f"Falha na API de IA (HTTP {exc.code}). {detail[:220]}"
    except Exception as exc:  # nosec B110
        return f"Falha na API de IA: {exc}"


def main() -> int:
    if not os.getenv("TELEGRAM_BOT_TOKEN"):
        print("Defina TELEGRAM_BOT_TOKEN antes de iniciar.")
        return 1

    print("Clara Telegram bot iniciado (polling).")
    print("Sem whitelist/autorizacao: qualquer chat que falar com o bot recebe resposta.")
    offset: int | None = None
    history: dict[int, list[dict[str, str]]] = defaultdict(list)

    while True:
        ok, updates, detail = get_telegram_updates(offset=offset, timeout=25)
        if not ok:
            print(detail)
            time.sleep(3)
            continue

        for up in updates:
            update_id = up.get("update_id")
            if isinstance(update_id, int):
                offset = update_id + 1

            chat_id, text = _extract_text(up)
            if chat_id is None or not text:
                continue

            if text.lower() in {"/start", "/help"}:
                intro = (
                    "Oi, eu sou a Clara. Posso te ajudar com tarefas, organizacao, "
                    "negocio e operacao. Me diga o que precisa."
                )
                send_telegram_message_to_chat(chat_id, intro)
                continue

            msgs = history[chat_id]
            if not msgs:
                msgs.append({"role": "system", "content": SYSTEM_PROMPT})
            msgs.append({"role": "user", "content": text})
            # janela curta para custo/latencia controlados
            compact = [msgs[0]] + msgs[-10:] if len(msgs) > 1 else msgs
            answer = _call_openai(compact)
            msgs.append({"role": "assistant", "content": answer})
            history[chat_id] = [msgs[0]] + msgs[-20:]
            send_telegram_message_to_chat(chat_id, answer)


if __name__ == "__main__":
    raise SystemExit(main())
