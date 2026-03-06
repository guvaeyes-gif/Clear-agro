from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pdfplumber

ROOT = Path(__file__).resolve().parents[2]
RAW_BANKS = ROOT / "data" / "raw" / "banks"

DATE_RE = re.compile(r"^(\d{2}/\d{2}/\d{4})\s+(.*)$")
MONEY_RE = re.compile(r"[-+]?\d{1,3}(?:\.\d{3})*,\d{2}")

HEADER_MARKERS = (
    "Lançamentos do período",
    "Data Lançamentos",
    "Saldo total",
    "Limite da conta",
    "Utilizado",
    "Disponível",
    "Agência",
    "Conta",
)

ACTION_HINTS = ("PIX ENVIADO", "PIX RECEBIDO", "PAGAMENTO DE BOLETO", "TED", "DOC", "TAR")


def parse_money_br(value: str) -> float:
    normalized = value.replace(".", "").replace(",", ".")
    return float(normalized)


def clean_spaces(text: str) -> str:
    return " ".join(text.split())


def should_buffer(line: str) -> bool:
    s = line.strip()
    if not s:
        return False
    if any(marker.lower() in s.lower() for marker in HEADER_MARKERS):
        return False
    if DATE_RE.match(s):
        return False
    if MONEY_RE.search(s):
        return False
    if len(s) > 90:
        return False
    return True


def parse_pdf(path: Path) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    prefix_buffer = ""
    seq = 1

    with pdfplumber.open(path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            for raw_line in text.splitlines():
                line = clean_spaces(raw_line)
                if not line:
                    continue

                m = DATE_RE.match(line)
                if not m:
                    if should_buffer(line):
                        prefix_buffer = line
                    continue

                data_txt, rest = m.group(1), m.group(2)
                amounts = MONEY_RE.findall(rest)
                if not amounts:
                    continue

                valor_txt = amounts[-1]
                valor = parse_money_br(valor_txt)

                desc = MONEY_RE.sub("", rest)
                desc = clean_spaces(desc)

                if prefix_buffer and desc.upper().startswith(ACTION_HINTS):
                    desc = f"{desc} {prefix_buffer}"

                rows.append(
                    {
                        "data": pd.to_datetime(data_txt, dayfirst=True, errors="coerce"),
                        "valor": valor,
                        "descricao": desc,
                        "entidade": "",
                        "bank": "itau",
                        "country": "BR",
                        "source": "banks_pdf",
                        "source_file": str(path.relative_to(ROOT)),
                        "page": page_num,
                        "raw_id": f"{path.stem}-{seq}",
                    }
                )
                seq += 1
                prefix_buffer = ""

    return pd.DataFrame(rows)


def main() -> int:
    pdfs = sorted(RAW_BANKS.glob("Extrato_*.pdf"))
    if not pdfs:
        print("No Itau PDF files found")
        return 0

    total_rows = 0
    generated = []
    for pdf in pdfs:
        df = parse_pdf(pdf)
        out_csv = pdf.with_suffix(".csv")
        df.to_csv(out_csv, index=False)
        total_rows += len(df)
        generated.append((out_csv, len(df)))

    for out_csv, rows in generated:
        print(f"{out_csv} | rows={rows}")
    print(f"Total rows={total_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
