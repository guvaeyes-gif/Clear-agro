from __future__ import annotations

from collections import defaultdict
import unicodedata

import pandas as pd


def _vendor_key(value: object) -> str:
    txt = str(value or "").strip().upper()
    if not txt:
        return ""
    txt = "".join(ch for ch in unicodedata.normalize("NFKD", txt) if not unicodedata.combining(ch))
    return " ".join(txt.split())


def _normalize_vendor_id_text(value: object) -> str:
    txt = str(value or "").strip()
    if not txt:
        return ""
    if txt.endswith(".0"):
        try:
            return str(int(float(txt)))
        except Exception:
            return txt[:-2]
    return txt


def _extract_vendor_id_from_label(value: object) -> str:
    txt = str(value or "").strip()
    if txt.endswith(")") and "(" in txt:
        possible_id = txt.rsplit("(", 1)[-1].rstrip(")").strip()
        if possible_id:
            return possible_id
    return ""


def _build_vendor_name_lookup(
    vendor_map: pd.DataFrame | None = None,
    vendor_alias_map: dict[str, str] | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    id_to_name: dict[str, str] = {}
    name_to_id: dict[str, str] = {}
    name_collisions: set[str] = set()

    if vendor_alias_map:
        for vendor_id, vendor_name in vendor_alias_map.items():
            vendor_id_txt = _normalize_vendor_id_text(vendor_id)
            vendor_name_txt = str(vendor_name or "").strip()
            if vendor_id_txt and vendor_name_txt:
                id_to_name.setdefault(vendor_id_txt, vendor_name_txt)

    if vendor_map is not None and not vendor_map.empty and {"vendedor_id", "vendedor"}.issubset(vendor_map.columns):
        vm = vendor_map.copy()
        vm["vendedor_id"] = vm["vendedor_id"].fillna("").astype(str).str.strip().map(_normalize_vendor_id_text)
        vm["vendedor"] = vm["vendedor"].fillna("").astype(str).str.strip()
        vm = vm[(vm["vendedor_id"] != "") & (vm["vendedor"] != "")]
        for _, row in vm.iterrows():
            vendor_id = str(row["vendedor_id"]).strip()
            vendor_name = str(row["vendedor"]).strip()
            if vendor_id and vendor_name:
                id_to_name.setdefault(vendor_id, vendor_name)
                name_key = _vendor_key(vendor_name)
                if not name_key or name_key in name_collisions:
                    continue
                if name_key in name_to_id and name_to_id[name_key] != vendor_id:
                    name_collisions.add(name_key)
                    name_to_id.pop(name_key, None)
                else:
                    name_to_id[name_key] = vendor_id

    for vendor_name_key in list(name_collisions):
        name_to_id.pop(vendor_name_key, None)

    return id_to_name, name_to_id


def canonical_vendor_name(
    value: object,
    vendor_map: pd.DataFrame | None = None,
    vendor_alias_map: dict[str, str] | None = None,
) -> str:
    txt = str(value or "").strip()
    if not txt:
        return ""
    if txt.upper() == "TODOS":
        return "TODOS"

    id_to_name, _ = _build_vendor_name_lookup(vendor_map, vendor_alias_map)
    vendor_id = _normalize_vendor_id_text(_extract_vendor_id_from_label(txt) or txt)
    if vendor_id:
        mapped_name = id_to_name.get(vendor_id, "")
        if mapped_name:
            return mapped_name
        base_name = txt.rsplit("(", 1)[0].strip()
        if base_name and not base_name.replace(".", "", 1).isdigit():
            return base_name
        return ""
    if txt in id_to_name:
        return id_to_name[txt]
    normalized_txt = _normalize_vendor_id_text(txt)
    if normalized_txt and normalized_txt in id_to_name:
        return id_to_name[normalized_txt]

    key = _vendor_key(txt)
    for candidate in id_to_name.values():
        if _vendor_key(candidate) == key:
            return candidate
    return txt


def build_vendor_selector_options(
    vendor_scores: dict[str, float],
    all_vendor_labels: set[str],
    vendor_map: pd.DataFrame | None = None,
    vendor_alias_map: dict[str, str] | None = None,
    *,
    show_inactive_vendors: bool = False,
) -> list[str]:
    score_by_name: dict[str, float] = defaultdict(float)

    for label, score in vendor_scores.items():
        name = canonical_vendor_name(label, vendor_map, vendor_alias_map)
        if not name or name == "TODOS":
            continue
        key = _vendor_key(name)
        if not key:
            continue
        score_by_name[name] += float(score)

    all_names: dict[str, str] = {}
    for label in all_vendor_labels:
        name = canonical_vendor_name(label, vendor_map, vendor_alias_map)
        if not name or name == "TODOS":
            continue
        key = _vendor_key(name)
        if not key:
            continue
        all_names.setdefault(key, name)

    active = sorted(
        [name for name, score in score_by_name.items() if score > 0],
        key=lambda name: (-score_by_name[name], _vendor_key(name)),
    )
    active_keys = {_vendor_key(name) for name in active}
    inactive = sorted(
        [name for key, name in all_names.items() if key not in active_keys],
        key=_vendor_key,
    )

    if show_inactive_vendors:
        vendor_list = active + [name for name in inactive if name not in active]
        if not vendor_list:
            vendor_list = sorted(all_names.values(), key=_vendor_key)
    else:
        vendor_list = active
        if not vendor_list:
            vendor_list = sorted(all_names.values(), key=_vendor_key)

    deduped: list[str] = []
    seen: set[str] = set()
    for name in vendor_list:
        key = _vendor_key(name)
        if key and key not in seen:
            seen.add(key)
            deduped.append(name)
    return ["TODOS", *deduped]


def normalize_vendor_identity(
    df: pd.DataFrame,
    vendor_map: pd.DataFrame | None = None,
    vendor_alias_map: dict[str, str] | None = None,
    *,
    vendor_id_col: str = "vendedor_id",
    vendor_name_col: str = "vendedor",
) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    if vendor_id_col not in out.columns and vendor_name_col not in out.columns:
        return out

    id_to_name, name_to_id = _build_vendor_name_lookup(vendor_map, vendor_alias_map)

    if vendor_id_col in out.columns:
        out[vendor_id_col] = out[vendor_id_col].fillna("").astype(str).str.strip().map(_normalize_vendor_id_text)
    else:
        out[vendor_id_col] = ""
    if vendor_name_col in out.columns:
        out[vendor_name_col] = out[vendor_name_col].fillna("").astype(str).str.strip()
    else:
        out[vendor_name_col] = ""

    def _looks_like_vendor_id(value: object) -> bool:
        txt = str(value or "").strip()
        if not txt:
            return False
        normalized = _normalize_vendor_id_text(txt)
        return normalized == txt and txt.replace(".", "", 1).isdigit()

    missing_name = out[vendor_name_col].eq("") | out[vendor_name_col].map(_looks_like_vendor_id)
    if missing_name.any():
        out.loc[missing_name, vendor_name_col] = out.loc[missing_name, vendor_id_col].map(id_to_name).fillna("")

    missing_id = out[vendor_id_col].eq("")
    if missing_id.any():
        id_from_name = out.loc[missing_id, vendor_name_col].map(lambda value: name_to_id.get(_vendor_key(value), ""))
        fill_mask = id_from_name.ne("")
        if fill_mask.any():
            out.loc[id_from_name.index[fill_mask], vendor_id_col] = id_from_name[fill_mask]

    return out
