import pandas as pd

from src import data


def test_apply_vendor_map_fills_missing_vendor_name(monkeypatch):
    monkeypatch.setattr(
        data,
        "load_bling_vendor_map",
        lambda: pd.DataFrame(
            [
                {"vendedor_id": "15596209223", "vendedor": "Ana", "empresa": "CZ"},
                {"vendedor_id": "15596236438", "vendedor": "Bruno", "empresa": "CR"},
            ]
        ),
    )

    df = pd.DataFrame(
        [
            {"vendedor_id": "15596209223", "vendedor": ""},
            {"vendedor_id": "15596236438", "vendedor": "Nome Manual"},
        ]
    )

    out = data._apply_vendor_map(df)

    assert out.loc[0, "vendedor"] == "Ana"
    assert out.loc[1, "vendedor"] == "Nome Manual"


def test_load_bling_realizado_uses_nfe_vendor_id_and_map(monkeypatch):
    monkeypatch.setattr(
        data,
        "_load_bling_nfe_rows",
        lambda years=None: pd.DataFrame(
            [
                {
                    "dataEmissao": "2026-03-10",
                    "valorNota": "1250.50",
                    "contato.nome": "Cliente A",
                    "vendedor.id": "15596209223",
                    "empresa": "CZ",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        data,
        "load_bling_vendor_map",
        lambda: pd.DataFrame(
            [{"vendedor_id": "15596209223", "vendedor": "Ana", "empresa": "CZ"}]
        ),
    )
    monkeypatch.setattr(data, "_append_nature_labels", lambda df: df)
    monkeypatch.setattr(data, "_map_vendedor_from_local_history", lambda df: df)
    data.load_bling_realizado.clear()

    out = data.load_bling_realizado()

    assert out.loc[0, "vendedor_id"] == "15596209223"
    assert out.loc[0, "vendedor"] == "Ana"
    assert out.loc[0, "receita"] == 1250.50
    assert out.loc[0, "origem"] == "bling_nfe"


def test_load_bling_realizado_falls_back_to_vendor_id_when_map_has_no_name(monkeypatch):
    monkeypatch.setattr(
        data,
        "_load_bling_nfe_rows",
        lambda years=None: pd.DataFrame(
            [
                {
                    "dataEmissao": "2026-03-10",
                    "valorNota": "500",
                    "contato.nome": "Cliente B",
                    "vendedor.id": "15596559039",
                    "empresa": "CZ",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        data,
        "load_bling_vendor_map",
        lambda: pd.DataFrame(
            [{"vendedor_id": "15596559039", "vendedor": "", "empresa": "CZ"}]
        ),
    )
    monkeypatch.setattr(data, "_append_nature_labels", lambda df: df)
    monkeypatch.setattr(data, "_map_vendedor_from_local_history", data._map_vendedor_from_local_history)
    data.load_bling_realizado.clear()

    out = data.load_bling_realizado()

    assert out.loc[0, "vendedor"] == "15596559039"


def test_apply_vendor_map_prefers_manual_name_when_available(monkeypatch):
    monkeypatch.setattr(
        data,
        "load_bling_vendor_map",
        lambda: pd.DataFrame(
            [
                {"vendedor_id": "15596209223", "vendedor": "", "empresa": "CZ"},
                {"vendedor_id": "15596209223", "vendedor": "Ana Paula", "empresa": "CZ"},
            ]
        ),
    )

    df = pd.DataFrame([{"vendedor_id": "15596209223", "vendedor": ""}])

    out = data._apply_vendor_map(df)

    assert out.loc[0, "vendedor"] == "Ana Paula"
