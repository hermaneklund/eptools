from pathlib import Path
import os

from io import BytesIO
import json
import pandas as pd
import sqlite3
import numpy as np
import unicodedata
from datetime import datetime

from fastapi import FastAPI, Request, File, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
try:
    import yfinance as yf
except Exception:
    yf = None

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}

BASE_DIR = Path(__file__).parent
EXCEL_PATH = Path(os.getenv("PORTFOLIO_EXCEL_PATH", r"C:\Users\HermanEklund\Desktop\E&P\Portfolio Data.xlsm"))
DB_PATH = Path(os.getenv("PORTFOLIO_DB_PATH", str(BASE_DIR / "data" / "portfolio.db")))
SHEETS = ["Mandat", "Taggar", "Detaljerat", "Strategi"]
DB_SHEETS = ["Detaljerat"]
DISPLAY_SHEETS = ["Detaljerat"]

DISPLAY_MAP = {
    "Detaljerat": [
        ("Instrument Name", "Innehav"),
        ("Available Count", "Antal"),
        ("Price", "Kurs"),
        ("Currency", "Valuta"),
        ("Värde i SEK", "Värde (sek)"),
        ("Modul", "Modul"),
        ("Tillgångsslag", "Tillgångsslag"),
        ("RG", "RG"),
    ],
}

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _find_header_row(raw_df: pd.DataFrame, header_keys: list[str]) -> int:
    for i in range(len(raw_df)):
        row = raw_df.iloc[i].astype(str).str.strip().str.lower()
        if any(row.eq(key).any() for key in header_keys):
            return i
    return 0


def _load_sheet_from_excel(sheet_name: str, excel_content: bytes | None = None) -> pd.DataFrame:
    excel_source = EXCEL_PATH if excel_content is None else BytesIO(excel_content)
    try:
        raw = pd.read_excel(excel_source, sheet_name=sheet_name, header=None, engine="openpyxl")
    except ValueError:
        return pd.DataFrame()
    if sheet_name == "Strategi":
        if raw.empty:
            return pd.DataFrame()
        header = raw.iloc[0].astype(str).str.strip().tolist()
        df = raw.iloc[1:].copy()
        df.columns = header
        df = df.dropna(axis=1, how="all")
        return df
    header_keys = ["number", "nummer"]
    if sheet_name == "Taggar":
        header_keys = ["short name", "kortnamn", "shortname"]
    header_row = _find_header_row(raw, header_keys)
    excel_source = EXCEL_PATH if excel_content is None else BytesIO(excel_content)
    try:
        df = pd.read_excel(excel_source, sheet_name=sheet_name, header=header_row, engine="openpyxl")
    except ValueError:
        return pd.DataFrame()
    df = df.dropna(axis=1, how="all")
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cursor.fetchone() is not None


def _load_sheet_from_db(sheet_name: str) -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    with sqlite3.connect(DB_PATH) as conn:
        if _table_exists(conn, sheet_name):
            return pd.read_sql_query(f'SELECT * FROM "{sheet_name}"', conn)
        table = sheet_name.lower()
        if not _table_exists(conn, table):
            return pd.DataFrame()
        return pd.read_sql_query(f'SELECT * FROM "{table}"', conn)


def _load_sheet(sheet_name: str) -> pd.DataFrame:
    if sheet_name in {"Mandat", "Taggar", "Strategi"}:
        return _load_sheet_from_db(sheet_name)
    if DB_PATH.exists():
        df = _load_sheet_from_db(sheet_name)
        if not df.empty:
            return df
    return _load_sheet_from_excel(sheet_name)


def _load_strategi() -> pd.DataFrame:
    if DB_PATH.exists():
        df = _load_sheet_from_db("Strategi")
        if not df.empty:
            return df
    return _load_sheet_from_excel("Strategi")


def _import_excel_to_db(excel_content: bytes | None = None) -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('DROP TABLE IF EXISTS "aggregerat"')
        for sheet in DB_SHEETS:
            df = _load_sheet_from_excel(sheet, excel_content=excel_content)
            if df.empty and len(df.columns) == 0:
                continue
            df.to_sql(sheet.lower(), conn, if_exists="replace", index=False)
        # Update Mandat boolean columns from Excel if provided
        mandat_bool_cols = ["RG7>25", "20%", "Akt>75", "Akt>25", "Alt>50", "Rä != 0", "Alt!= 0"]
        mandat_update = _load_sheet_from_excel("Mandat", excel_content=excel_content)
        if not mandat_update.empty and "Number" in mandat_update.columns:
            if _table_exists(conn, "mandat"):
                db_mandat = pd.read_sql_query('SELECT * FROM "mandat"', conn)
                number_col = "Number" if "Number" in db_mandat.columns else "Nummer"
                db_mandat[number_col] = db_mandat[number_col].apply(_normalize_number_value)
                mandat_update[number_col] = mandat_update["Number"].apply(_normalize_number_value)
                cols_to_use = [c for c in mandat_bool_cols if c in mandat_update.columns]
                if cols_to_use:
                    for col in cols_to_use:
                        mandat_update[col] = mandat_update[col].apply(
                            lambda v: 0 if v == "" or pd.isna(v) else 1
                        )
                    merged = db_mandat.merge(
                        mandat_update[[number_col] + cols_to_use],
                        on=number_col,
                        how="left",
                        suffixes=("", "_xl"),
                    )
                    for col in cols_to_use:
                        xl_col = f"{col}_xl"
                        if xl_col in merged.columns:
                            merged[col] = merged[xl_col].fillna(merged[col])
                            merged.drop(columns=[xl_col], inplace=True)
                    merged.to_sql("mandat", conn, if_exists="replace", index=False)
        conn.execute(
            'CREATE TABLE IF NOT EXISTS "_meta" (key TEXT PRIMARY KEY, value TEXT)'
        )
        conn.execute(
            'INSERT OR REPLACE INTO "_meta" (key, value) VALUES (?, ?)',
            ("last_import", datetime.now().strftime("%Y-%m-%d %H:%M")),
        )
        conn.commit()


def _import_db_excel_to_db(excel_content: bytes) -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    workbook = pd.ExcelFile(BytesIO(excel_content), engine="openpyxl")
    with sqlite3.connect(DB_PATH) as conn:
        for sheet in workbook.sheet_names:
            table_name = str(sheet).strip()
            if not table_name:
                continue
            df = pd.read_excel(workbook, sheet_name=sheet, engine="openpyxl")
            if len(df.columns) == 0:
                continue
            df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.execute(
            'CREATE TABLE IF NOT EXISTS "_meta" (key TEXT PRIMARY KEY, value TEXT)'
        )
        conn.execute(
            'INSERT OR REPLACE INTO "_meta" (key, value) VALUES (?, ?)',
            ("last_import", datetime.now().strftime("%Y-%m-%d %H:%M")),
        )
        conn.execute(
            'INSERT OR REPLACE INTO "_meta" (key, value) VALUES (?, ?)',
            ("last_import_source", "db_excel_upload"),
        )
        conn.commit()


def _get_last_import() -> str:
    if not DB_PATH.exists():
        return "Ej importerat"
    with sqlite3.connect(DB_PATH) as conn:
        if not _table_exists(conn, "_meta"):
            return "Ej importerat"
        row = conn.execute(
            'SELECT value FROM "_meta" WHERE key = ?',
            ("last_import",),
        ).fetchone()
        return row[0] if row and row[0] else "Ej importerat"


templates.env.globals["last_import"] = _get_last_import


FLAG_COLUMNS = ["dynamisk", "coresv", "corevä", "edge", "alts"]
FLAG_DB_MAP = {
    "dynamisk": "dynamisk",
    "coresv": "coresv",
    "corevä": "coreva",
    "edge": "edge",
    "alts": "alts",
}


def _load_mandat_flags() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    with sqlite3.connect(DB_PATH) as conn:
        if not _table_exists(conn, "mandat_flags"):
            return pd.DataFrame()
        return pd.read_sql_query('SELECT * FROM "mandat_flags"', conn)


def _save_mandat_flags(flags: dict[str, dict[str, int]]) -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    existing = _load_mandat_flags()
    existing_map = {}
    if not existing.empty and "number" in existing.columns:
        for _, row in existing.iterrows():
            existing_map[str(row.get("number", "")).strip()] = row.to_dict()
    for number, values in flags.items():
        row = existing_map.get(str(number).strip(), {})
        row["number"] = str(number).strip()
        for col in FLAG_COLUMNS:
            row[FLAG_DB_MAP[col]] = int(values.get(col, row.get(FLAG_DB_MAP[col], 0)))
        existing_map[str(number).strip()] = row
    df = pd.DataFrame(list(existing_map.values()))
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql("mandat_flags", conn, if_exists="replace", index=False)


def _load_mandat_dyn() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    with sqlite3.connect(DB_PATH) as conn:
        if not _table_exists(conn, "mandat_dyn"):
            return pd.DataFrame()
        return pd.read_sql_query('SELECT * FROM "mandat_dyn"', conn)


def _save_mandat_dyn(rows: list[dict]) -> None:
    if not rows:
        return
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql("mandat_dyn", conn, if_exists="replace", index=False)


def _load_mandat_table() -> pd.DataFrame:
    return _load_sheet_from_db("Mandat")


def _save_mandat_table(df: pd.DataFrame) -> None:
    if df.empty:
        return
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql("mandat", conn, if_exists="replace", index=False)


def _prepare_mandat_rows_for_compliance(q: str = "") -> tuple[list[dict], str]:
    df = _load_sheet("Mandat")
    if df.empty:
        return [], "Number"
    number_col = "Number" if "Number" in df.columns else "Nummer"
    for col in FLAG_COLUMNS:
        if col not in df.columns:
            df[col] = 0
    for col in MANDAT_BOOL_COLUMNS:
        if col not in df.columns:
            df[col] = 0
    bool_cols = [
        "RG7>25",
        "20%",
        "Akt>75",
        "Akt>25",
        "Alt>50",
        "Rä != 0",
        "Alt!= 0",
        "MK",
        "Outlier",
        "Placeringsriktlinjer",
    ]
    falsy_values = {"", "0", "false", "False", "FALSE", "nan", "NaN", None}
    for col in bool_cols:
        if col in df.columns:
            def _to_bool_int(v):
                if pd.isna(v):
                    return 0
                text = str(v).strip()
                if text in falsy_values:
                    return 0
                try:
                    num = float(text)
                    return 0 if num == 0 else 1
                except (TypeError, ValueError):
                    return 1
            df[col] = df[col].apply(_to_bool_int)
    flags_df = _load_mandat_flags()
    if not flags_df.empty and number_col in df.columns and "number" in flags_df.columns:
        flags_df = flags_df.rename(
            columns={
                "number": number_col,
                "dynamisk": "dynamisk",
                "coresv": "coresv",
                "coreva": "corevä",
                "edge": "edge",
                "alts": "alts",
            }
        )
        flags_df[number_col] = flags_df[number_col].astype(str).str.strip()
        df[number_col] = df[number_col].astype(str).str.strip()
        df = df.merge(flags_df, on=number_col, how="left", suffixes=("", "_flag"))
        for col in FLAG_COLUMNS:
            flag_col = f"{col}_flag"
            if flag_col in df.columns:
                df[col] = df[flag_col].fillna(df[col])
                df.drop(columns=[flag_col], inplace=True)
        for col in FLAG_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df = df.where(pd.notna(df), "")
    if q and number_col in df.columns:
        df = df[df[number_col].astype(str).str.strip() == q.strip()]
    rows = df.to_dict(orient="records")
    for row in rows:
        row["_row_key"] = _normalize_number_value(row.get(number_col, ""))
    return rows, number_col


def _build_compliance_rows(rows: list[dict], number_col: str) -> list[dict]:
    compliance_rows = []
    detaljerat = _load_sheet("Detaljerat")
    taggar_df = _load_sheet("Taggar")
    taggar_map = {}
    currency_map = {}
    if "Short Name" in taggar_df.columns:
        for _, row in taggar_df.iterrows():
            key = _normalize_key(row.get("Short Name", ""))
            if not key:
                continue
            taggar_map[key] = row.to_dict()
            kurs = pd.to_numeric(row.get("Kurs", None), errors="coerce")
            if pd.notna(kurs):
                currency_map[key] = float(kurs)
    if not detaljerat.empty and "Number" in detaljerat.columns:
        detaljerat["__number"] = detaljerat["Number"].apply(_normalize_number_value)
        mandat_numbers = {
            _normalize_number_value(r.get(number_col, ""))
            for r in rows
            if _normalize_number_value(r.get(number_col, ""))
        }
        if mandat_numbers:
            detaljerat = detaljerat[detaljerat["__number"].isin(mandat_numbers)]
        detaljerat["__tillgang"] = detaljerat["Short Name"].apply(
            lambda s: str(taggar_map.get(_normalize_key(s), {}).get("Tillgångsslag", "")).strip().lower()
        )
        detaljerat["__rg"] = detaljerat["Short Name"].apply(
            lambda s: _to_float(taggar_map.get(_normalize_key(s), {}).get("RG", ""))
        )
        detaljerat["__modul"] = detaljerat["Short Name"].apply(
            lambda s: str(taggar_map.get(_normalize_key(s), {}).get("Modul", "")).strip().lower()
        ).replace({"": "övrigt", "nan": "övrigt"})
        if "Instrument Type" in detaljerat.columns:
            fallback = (
                detaljerat["Instrument Type"]
                .astype(str)
                .str.strip()
                .str.lower()
                .map({"share": "aktier", "bond": "ränta", "fund": "aktier", "etf": "aktier"})
                .fillna("")
            )
            detaljerat["__tillgang"] = detaljerat["__tillgang"].where(
                detaljerat["__tillgang"] != "", fallback
            )
        counts = _to_float_series(detaljerat.get("Available Count", pd.Series([0] * len(detaljerat)))).fillna(0)
        prices = _to_float_series(detaljerat.get("Price", pd.Series([0] * len(detaljerat)))).fillna(0)
        base_value = counts * prices
        base_value = base_value.where(detaljerat["__modul"] != "fixed income", base_value / 100)
        if "Currency" in detaljerat.columns:
            rates = detaljerat["Currency"].apply(
                lambda c: currency_map.get(_normalize_key(c), 1.0)
            )
            base_value = base_value * _to_float_series(rates).fillna(1.0)
        detaljerat["__value"] = base_value.fillna(0)

        mandat_rows = [r for r in rows if str(r.get("Mandat", "")).strip().lower() == "ränta"]
        mandate_map = {
            _normalize_number_value(r.get(number_col, "")): r
            for r in rows
            if _normalize_number_value(r.get(number_col, ""))
        }
        totals_all = (
            detaljerat.groupby("__number")["__value"].sum().to_dict()
            if not detaljerat.empty
            else {}
        )

        # Rule: Ränta should not have Aktier/Alternativa
        for r in mandat_rows:
            num = _normalize_number_value(r.get(number_col, ""))
            if not num:
                continue
            sub = detaljerat[detaljerat["__number"] == num]
            if sub.empty:
                continue
            bad = sub[sub["__tillgang"].isin(["aktier", "alternativa"])]
            if not bad.empty:
                bad_names = (
                    bad["Short Name"]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .loc[lambda s: s != ""]
                    .unique()
                    .tolist()
                )
                reason = ", ".join(bad_names[:5]) if bad_names else ""
                compliance_rows.append(
                    {
                        "Number": num,
                        "Kund": r.get("Kund", ""),
                        "Mandat": r.get("Mandat", ""),
                        "Mandatnotering": r.get("Mandatnotering", ""),
                        "Rule": "Akt/Alt !=0",
                        "Antal": "",
                        "Innehav": reason,
                    }
                )

        # Rule: Any holding > 20% (excl. valuta)
        for num, total in totals_all.items():
            if not total:
                continue
            sub = detaljerat[detaljerat["__number"] == num]
            if sub.empty:
                continue
            non_valuta = sub[sub["__tillgang"] != "valuta"]
            if non_valuta.empty:
                continue
            max_share = non_valuta["__value"].max() / total if total else 0
            if max_share > 0.20:
                max_row = non_valuta.loc[non_valuta["__value"].idxmax()]
                max_name = str(max_row.get("Short Name", "")).strip()
                share_text = f"{_format_number(max_share * 100, 1)}%"
                reason = f"{max_name} ({share_text})" if max_name else share_text
                mandat_row = mandate_map.get(num, {})
                placeringsriktlinjer = _to_float(mandat_row.get("Placeringsriktlinjer", 0)) or 0
                flag_20 = _to_float(mandat_row.get("20%", 0)) or 0
                godk = pd.to_datetime(mandat_row.get("Godkännande", ""), errors="coerce")
                recent_godk = False
                if pd.notna(godk):
                    recent_godk = (datetime.now() - godk).days < 365
                if not (flag_20 == 1 and (placeringsriktlinjer == 1 or recent_godk)):
                    compliance_rows.append(
                        {
                            "Number": num,
                            "Kund": mandat_row.get("Kund", ""),
                            "Mandat": mandat_row.get("Mandat", ""),
                            "Mandatnotering": mandat_row.get("Mandatnotering", ""),
                            "Rule": "EI>20%",
                            "Antal": "",
                            "Innehav": reason,
                        }
                    )

        # Rule: Balanserad defensiv limits
        mandat_rows_bd = [r for r in rows if str(r.get("Mandat", "")).strip().lower() == "balanserad defensiv"]
        if mandat_rows_bd:
            for r in mandat_rows_bd:
                num = _normalize_number_value(r.get(number_col, ""))
                if not num:
                    continue
                total = totals_all.get(num, 0)
                if not total:
                    continue
                sub = detaljerat[detaljerat["__number"] == num]
                if sub.empty:
                    continue
                aktier_sum = sub[sub["__tillgang"] == "aktier"]["__value"].sum()
                alternativa_sum = sub[sub["__tillgang"] == "alternativa"]["__value"].sum()
                rg7_sum = sub[sub["__rg"] == 7]["__value"].sum()
                mandat_row = mandate_map.get(num, {})
                if aktier_sum / total > 0.25:
                    placeringsriktlinjer = _to_float(mandat_row.get("Placeringsriktlinjer", 0)) or 0
                    flag_akt25 = _to_float(mandat_row.get("Akt>25", 0)) or 0
                    godk = pd.to_datetime(mandat_row.get("Godkännande", ""), errors="coerce")
                    recent_godk = False
                    if pd.notna(godk):
                        recent_godk = (datetime.now() - godk).days < 365
                    if not (flag_akt25 == 1 and (placeringsriktlinjer == 1 or recent_godk)):
                        compliance_rows.append(
                            {
                                "Number": num,
                                "Kund": mandat_row.get("Kund", ""),
                                "Mandat": mandat_row.get("Mandat", ""),
                                "Mandatnotering": mandat_row.get("Mandatnotering", ""),
                                "Rule": "Balanserad defensiv: Aktier > 25%",
                                "Antal": "",
                                "Innehav": f"{_format_number((aktier_sum / total) * 100, 1)}%",
                            }
                        )
                if alternativa_sum / total > 0.25:
                    compliance_rows.append(
                        {
                            "Number": num,
                            "Kund": mandat_row.get("Kund", ""),
                            "Mandat": mandat_row.get("Mandat", ""),
                            "Mandatnotering": mandat_row.get("Mandatnotering", ""),
                            "Rule": "Balanserad defensiv: Alternativa > 25%",
                            "Antal": "",
                            "Innehav": f"{_format_number((alternativa_sum / total) * 100, 1)}%",
                        }
                    )
                if rg7_sum / total > 0.10:
                    compliance_rows.append(
                        {
                            "Number": num,
                            "Kund": mandat_row.get("Kund", ""),
                            "Mandat": mandat_row.get("Mandat", ""),
                            "Mandatnotering": mandat_row.get("Mandatnotering", ""),
                            "Rule": "Balanserad defensiv: RG7 > 10%",
                            "Antal": "",
                            "Innehav": f"{_format_number((rg7_sum / total) * 100, 1)}%",
                        }
                    )

        # Rule: Balanserad offensiv limits
        mandat_rows_bo = [r for r in rows if str(r.get("Mandat", "")).strip().lower() == "balanserad offensiv"]
        if mandat_rows_bo:
            for r in mandat_rows_bo:
                num = _normalize_number_value(r.get(number_col, ""))
                if not num:
                    continue
                total = totals_all.get(num, 0)
                if not total:
                    continue
                sub = detaljerat[detaljerat["__number"] == num]
                if sub.empty:
                    continue
                aktier_sum = sub[sub["__tillgang"] == "aktier"]["__value"].sum()
                alternativa_sum = sub[sub["__tillgang"] == "alternativa"]["__value"].sum()
                rg7_sum = sub[sub["__rg"] == 7]["__value"].sum()
                mandat_row = mandate_map.get(num, {})
                if aktier_sum / total > 0.75:
                    placeringsriktlinjer = _to_float(mandat_row.get("Placeringsriktlinjer", 0)) or 0
                    flag_akt75 = _to_float(mandat_row.get("Akt>75", 0)) or 0
                    godk = pd.to_datetime(mandat_row.get("Godkännande", ""), errors="coerce")
                    recent_godk = False
                    if pd.notna(godk):
                        recent_godk = (datetime.now() - godk).days < 365
                    if not (flag_akt75 == 1 and (placeringsriktlinjer == 1 or recent_godk)):
                        compliance_rows.append(
                            {
                                "Number": num,
                                "Kund": mandat_row.get("Kund", ""),
                                "Mandat": mandat_row.get("Mandat", ""),
                                "Mandatnotering": mandat_row.get("Mandatnotering", ""),
                                "Rule": "Balanserad offensiv: Aktier > 75%",
                                "Antal": "",
                                "Innehav": f"{_format_number((aktier_sum / total) * 100, 1)}%",
                            }
                        )
                if alternativa_sum / total > 0.50:
                    placeringsriktlinjer = _to_float(mandat_row.get("Placeringsriktlinjer", 0)) or 0
                    flag_alt50 = _to_float(mandat_row.get("Alt>50", 0)) or 0
                    godk = pd.to_datetime(mandat_row.get("Godkännande", ""), errors="coerce")
                    recent_godk = False
                    if pd.notna(godk):
                        recent_godk = (datetime.now() - godk).days < 365
                    if not (flag_alt50 == 1 and (placeringsriktlinjer == 1 or recent_godk)):
                        compliance_rows.append(
                            {
                                "Number": num,
                                "Kund": mandat_row.get("Kund", ""),
                                "Mandat": mandat_row.get("Mandat", ""),
                                "Mandatnotering": mandat_row.get("Mandatnotering", ""),
                                "Rule": "Balanserad offensiv: Alternativa > 50%",
                                "Antal": "",
                                "Innehav": f"{_format_number((alternativa_sum / total) * 100, 1)}%",
                            }
                        )
                if rg7_sum / total > 0.25:
                    placeringsriktlinjer = _to_float(mandat_row.get("Placeringsriktlinjer", 0)) or 0
                    flag_rg7 = _to_float(mandat_row.get("RG7>25", 0)) or 0
                    godk = pd.to_datetime(mandat_row.get("Godkännande", ""), errors="coerce")
                    recent_godk = False
                    if pd.notna(godk):
                        recent_godk = (datetime.now() - godk).days < 365
                    if not (flag_rg7 == 1 and (placeringsriktlinjer == 1 or recent_godk)):
                        compliance_rows.append(
                            {
                                "Number": num,
                                "Kund": mandat_row.get("Kund", ""),
                                "Mandat": mandat_row.get("Mandat", ""),
                                "Mandatnotering": mandat_row.get("Mandatnotering", ""),
                                "Rule": "RG7>25%",
                                "Antal": "",
                                "Innehav": f"{_format_number((rg7_sum / total) * 100, 1)}%",
                            }
                        )

        # Rule: Offensiv limits
        mandat_rows_off = [r for r in rows if str(r.get("Mandat", "")).strip().lower() == "offensiv"]
        if mandat_rows_off:
            for r in mandat_rows_off:
                num = _normalize_number_value(r.get(number_col, ""))
                if not num:
                    continue
                total = totals_all.get(num, 0)
                if not total:
                    continue
                sub = detaljerat[detaljerat["__number"] == num]
                if sub.empty:
                    continue
                aktier_sum = sub[sub["__tillgang"] == "aktier"]["__value"].sum()
                alternativa_sum = sub[sub["__tillgang"] == "alternativa"]["__value"].sum()
                mandat_row = mandate_map.get(num, {})
                if aktier_sum / total > 0.75:
                    placeringsriktlinjer = _to_float(mandat_row.get("Placeringsriktlinjer", 0)) or 0
                    flag_akt75 = _to_float(mandat_row.get("Akt>75", 0)) or 0
                    godk = pd.to_datetime(mandat_row.get("Godkännande", ""), errors="coerce")
                    recent_godk = False
                    if pd.notna(godk):
                        recent_godk = (datetime.now() - godk).days < 365
                    if not (flag_akt75 == 1 and (placeringsriktlinjer == 1 or recent_godk)):
                        compliance_rows.append(
                            {
                                "Number": num,
                                "Kund": mandat_row.get("Kund", ""),
                                "Mandat": mandat_row.get("Mandat", ""),
                                "Mandatnotering": mandat_row.get("Mandatnotering", ""),
                                "Rule": "Offensiv: Aktier > 75%",
                                "Antal": "",
                                "Innehav": f"{_format_number((aktier_sum / total) * 100, 1)}%",
                            }
                        )
                if alternativa_sum / total > 0.50:
                    placeringsriktlinjer = _to_float(mandat_row.get("Placeringsriktlinjer", 0)) or 0
                    flag_alt50 = _to_float(mandat_row.get("Alt>50", 0)) or 0
                    godk = pd.to_datetime(mandat_row.get("Godkännande", ""), errors="coerce")
                    recent_godk = False
                    if pd.notna(godk):
                        recent_godk = (datetime.now() - godk).days < 365
                    if not (flag_alt50 == 1 and (placeringsriktlinjer == 1 or recent_godk)):
                        compliance_rows.append(
                            {
                                "Number": num,
                                "Kund": mandat_row.get("Kund", ""),
                                "Mandat": mandat_row.get("Mandat", ""),
                                "Mandatnotering": mandat_row.get("Mandatnotering", ""),
                                "Rule": "Alt>50%",
                                "Antal": "",
                                "Innehav": f"{_format_number((alternativa_sum / total) * 100, 1)}%",
                            }
                        )

        # Rule: Aktier mandates
        mandat_rows_akt = [r for r in rows if str(r.get("Mandat", "")).strip().lower() == "aktier"]
        if mandat_rows_akt:
            for r in mandat_rows_akt:
                num = _normalize_number_value(r.get(number_col, ""))
                if not num:
                    continue
                total = totals_all.get(num, 0)
                if not total:
                    continue
                sub = detaljerat[detaljerat["__number"] == num]
                if sub.empty:
                    continue
                rg7_sum = sub[sub["__rg"] == 7]["__value"].sum()
                mandat_row = mandate_map.get(num, {})
                fi_sum = sub[sub["__modul"] == "fixed income"]["__value"].sum()
                alt_sum = sub[sub["__tillgang"] == "alternativa"]["__value"].sum()

                if fi_sum / total > 0:
                    placeringsriktlinjer = _to_float(mandat_row.get("Placeringsriktlinjer", 0)) or 0
                    flag_ranta = _to_float(mandat_row.get("Rä != 0", 0)) or 0
                    godk = pd.to_datetime(mandat_row.get("Godkännande", ""), errors="coerce")
                    recent_godk = False
                    if pd.notna(godk):
                        recent_godk = (datetime.now() - godk).days < 365
                    if not (flag_ranta == 1 and (placeringsriktlinjer == 1 or recent_godk)):
                        compliance_rows.append(
                            {
                                "Number": num,
                                "Kund": mandat_row.get("Kund", ""),
                                "Mandat": mandat_row.get("Mandat", ""),
                                "Mandatnotering": mandat_row.get("Mandatnotering", ""),
                                "Rule": "Rä!=0",
                                "Antal": "",
                                "Innehav": f"{_format_number((fi_sum / total) * 100, 1)}%",
                            }
                        )
                if alt_sum / total > 0:
                    placeringsriktlinjer = _to_float(mandat_row.get("Placeringsriktlinjer", 0)) or 0
                    flag_alt = _to_float(mandat_row.get("Alt!= 0", 0)) or 0
                    godk = pd.to_datetime(mandat_row.get("Godkännande", ""), errors="coerce")
                    recent_godk = False
                    if pd.notna(godk):
                        recent_godk = (datetime.now() - godk).days < 365
                    if not (flag_alt == 1 and (placeringsriktlinjer == 1 or recent_godk)):
                        compliance_rows.append(
                            {
                                "Number": num,
                                "Kund": mandat_row.get("Kund", ""),
                                "Mandat": mandat_row.get("Mandat", ""),
                                "Mandatnotering": mandat_row.get("Mandatnotering", ""),
                                "Rule": "Alt!=0",
                                "Antal": "",
                                "Innehav": f"{_format_number((alt_sum / total) * 100, 1)}%",
                            }
                        )
                if rg7_sum / total > 0.25:
                    placeringsriktlinjer = _to_float(mandat_row.get("Placeringsriktlinjer", 0)) or 0
                    flag_rg7 = _to_float(mandat_row.get("RG7>25", 0)) or 0
                    godk = pd.to_datetime(mandat_row.get("Godkännande", ""), errors="coerce")
                    recent_godk = False
                    if pd.notna(godk):
                        recent_godk = (datetime.now() - godk).days < 365
                    if not (flag_rg7 == 1 and (placeringsriktlinjer == 1 or recent_godk)):
                        compliance_rows.append(
                            {
                                "Number": num,
                                "Kund": mandat_row.get("Kund", ""),
                                "Mandat": mandat_row.get("Mandat", ""),
                                "Mandatnotering": mandat_row.get("Mandatnotering", ""),
                                "Rule": "Aktier: RG7 > 25%",
                                "Antal": "",
                                "Innehav": f"{_format_number((rg7_sum / total) * 100, 1)}%",
                            }
                        )

    return compliance_rows


TAGGAR_COLUMNS = ["Short Name", "Modul", "Tillgångsslag", "RG", "Kurs", "Modellnamn", "Api", "Sektor", "FX"]
MANDAT_BOOL_COLUMNS = ["dynamisk", "coresv", "corevä", "edge", "alts", "RG7>25", "20%", "Akt>75", "Akt>25", "Alt>50", "Rä != 0", "Alt!= 0"]


def _load_taggar_table() -> pd.DataFrame:
    df = _load_sheet_from_db("Taggar")
    if df.empty:
        df = _load_sheet_from_excel("Taggar")
    if df.empty:
        return df
    if "row_id" not in df.columns:
        df = df.copy()
        df["row_id"] = range(1, len(df) + 1)
    if "FX" in TAGGAR_COLUMNS:
        detaljerat = _load_sheet("Detaljerat")
        fx_map = {}
        if not detaljerat.empty and "Short Name" in detaljerat.columns and "Currency" in detaljerat.columns:
            fx_series = (
                detaljerat[["Short Name", "Currency"]]
                .dropna()
                .assign(
                    short=detaljerat["Short Name"].astype(str).str.strip(),
                    curr=detaljerat["Currency"].astype(str).str.strip(),
                )
            )
            fx_series = fx_series[(fx_series["short"] != "") & (fx_series["curr"] != "")]
            if not fx_series.empty:
                fx_map = (
                    fx_series.groupby("short")["curr"]
                    .agg(lambda s: s.value_counts().index[0])
                    .to_dict()
                )
        df = df.copy()
        df["FX"] = df["Short Name"].astype(str).str.strip().map(fx_map).fillna("")
    if "Sektor" in TAGGAR_COLUMNS and "Sektor" not in df.columns:
        df = df.copy()
        df["Sektor"] = ""
    cols = [c for c in TAGGAR_COLUMNS if c in df.columns]
    df = df[["row_id"] + cols]
    return df


def _save_taggar_table(df: pd.DataFrame) -> None:
    if df.empty:
        return
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql("taggar", conn, if_exists="replace", index=False)


@app.post("/mandat-flags")
async def mandat_flags(request: Request):
    form = await request.form()
    flags: dict[str, dict[str, int]] = {}
    for key, value in form.multi_items():
        if key.startswith("flag__"):
            try:
                _, number, col = key.split("__", 2)
            except ValueError:
                continue
            if col not in FLAG_COLUMNS:
                continue
            flags.setdefault(number, {})
            flags[number][col] = 1 if str(value) == "1" else 0

    if flags:
        _save_mandat_flags(flags)

    referer = request.headers.get("referer", "/mandat")
    return RedirectResponse(referer, status_code=303)


@app.post("/mandat-save-row")
async def mandat_save_row(request: Request):
    form = await request.form()
    raw_number = form.get("row_id")
    if not raw_number:
        return RedirectResponse(request.headers.get("referer", "/mandat"), status_code=303)
    number = _normalize_number_value(raw_number)

    df = _load_mandat_table()
    if df.empty:
        return RedirectResponse(request.headers.get("referer", "/mandat"), status_code=303)

    number_col = "Number" if "Number" in df.columns else "Nummer"
    df[number_col] = df[number_col].apply(_normalize_number_value)
    mask = df[number_col] == number
    if not mask.any():
        return RedirectResponse(request.headers.get("referer", "/mandat"), status_code=303)
    for col in MANDAT_BOOL_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Update main Mandat table fields
    for col in df.columns:
        key = f"row__{raw_number}__{col}"
        if col in MANDAT_BOOL_COLUMNS:
            df.loc[mask, col] = 1 if key in form else 0
        elif key in form:
            df.loc[mask, col] = _coerce_cell_for_column(df, col, form.get(key))

    _save_mandat_table(df)

    # Update flags table if present in form
    flags = {}
    for col in FLAG_COLUMNS:
        key = f"row__{raw_number}__{col}"
        flags[col] = 1 if key in form else 0
    if flags:
        _save_mandat_flags({number: flags})

    referer = request.headers.get("referer", "/mandat")
    return RedirectResponse(referer, status_code=303)


@app.post("/mandat-add")
async def mandat_add(request: Request):
    form = await request.form()
    df = _load_mandat_table()
    number_col = "Number" if "Number" in df.columns else "Nummer"
    number = str(form.get(number_col, "")).strip()
    if not number:
        return RedirectResponse(request.headers.get("referer", "/mandat"), status_code=303)
    if df.empty:
        df = pd.DataFrame(columns=df.columns if len(df.columns) else [number_col])
    row = {}
    for col in df.columns:
        if col in MANDAT_BOOL_COLUMNS:
            row[col] = 1 if col in form else 0
        else:
            row[col] = _coerce_cell_for_column(df, col, form.get(col, ""))
    row[number_col] = number
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    _save_mandat_table(df)
    return RedirectResponse(request.headers.get("referer", "/mandat"), status_code=303)


@app.post("/mandat-delete")
async def mandat_delete(request: Request):
    form = await request.form()
    number = str(form.get("row_id", "")).strip()
    df = _load_mandat_table()
    if df.empty or not number:
        return RedirectResponse(request.headers.get("referer", "/mandat"), status_code=303)
    number_col = "Number" if "Number" in df.columns else "Nummer"
    df[number_col] = df[number_col].astype(str).str.strip()
    df = df[df[number_col] != number]
    _save_mandat_table(df)
    return RedirectResponse(request.headers.get("referer", "/mandat"), status_code=303)


@app.post("/taggar-save")
async def taggar_save(request: Request):
    form = await request.form()
    df = _load_taggar_table()
    if df.empty:
        df = pd.DataFrame(columns=["row_id"] + TAGGAR_COLUMNS)

    row_id = form.get("row_id")
    if row_id:
        row = {"row_id": int(row_id)}
        for col in TAGGAR_COLUMNS:
            if col == "FX":
                continue
            raw_val = form.get(f"row__{row_id}__{col}", "")
            if col == "Kurs":
                row[col] = _to_float(raw_val)
            else:
                row[col] = raw_val
        if (df["row_id"].astype(str) == str(row_id)).any():
            for col in TAGGAR_COLUMNS:
                if col == "FX":
                    continue
                df.loc[df["row_id"].astype(str) == str(row_id), col] = row.get(col, df[col])
        else:
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

    new_values = {}
    for col in TAGGAR_COLUMNS:
        if col == "FX":
            new_values[col] = ""
            continue
        raw_val = form.get(f"new__{col}", "")
        new_values[col] = _to_float(raw_val) if col == "Kurs" else raw_val
    def _has_value(v) -> bool:
        if v is None:
            return False
        if isinstance(v, float) and np.isnan(v):
            return False
        return str(v).strip() != ""

    if any(_has_value(v) for v in new_values.values()):
        next_id = (
            df["row_id"].max() + 1
            if not df.empty and pd.notna(df["row_id"].max())
            else 1
        )
        new_row = {"row_id": int(next_id), **new_values}
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    if not df.empty:
        _save_taggar_table(df)

    referer = request.headers.get("referer", "/taggar")
    return RedirectResponse(referer, status_code=303)


@app.post("/taggar-delete")
async def taggar_delete(request: Request):
    form = await request.form()
    row_id = form.get("row_id")
    df = _load_taggar_table()
    if row_id and not df.empty:
        df = df[df["row_id"].astype(str) != str(row_id)]
        _save_taggar_table(df)
    referer = request.headers.get("referer", "/taggar")
    return RedirectResponse(referer, status_code=303)


@app.post("/taggar-update-kurs")
def taggar_update_kurs(request: Request):
    if yf is None:
        referer = request.headers.get("referer", "/taggar")
        return RedirectResponse(referer, status_code=303)
    df = _load_taggar_table()
    if df.empty or "Api" not in df.columns:
        referer = request.headers.get("referer", "/taggar")
        return RedirectResponse(referer, status_code=303)
    if "Kurs" not in df.columns:
        df["Kurs"] = pd.NA
    df["Kurs"] = pd.to_numeric(df["Kurs"], errors="coerce").astype(object)
    price_cache = {}
    for idx, row in df.iterrows():
        ticker = str(row.get("Api", "")).strip()
        if not ticker or ticker in {"-", "nan", "NaN", "None"}:
            continue
        if ticker in price_cache:
            df.at[idx, "Kurs"] = float(price_cache[ticker])
            continue
        try:
            t = yf.Ticker(ticker)
            fast = getattr(t, "fast_info", {}) or {}
            price = fast.get("last_price") or fast.get("lastPrice")
            if price is None:
                hist = t.history(period="1d")
                if not hist.empty:
                    price = float(hist["Close"].iloc[-1])
            if price is not None:
                price = round(float(price), 2)
                price_cache[ticker] = float(price)
                df.at[idx, "Kurs"] = float(price)
        except Exception:
            continue
    _save_taggar_table(df)
    referer = request.headers.get("referer", "/taggar")
    return RedirectResponse(referer, status_code=303)


@app.post("/coresvdata-add")
async def coresvdata_add(request: Request):
    form = await request.form()
    total_value = _to_float(form.get("total_value")) or 0
    today = datetime.now().strftime("%Y-%m-%d")
    omxs30 = None
    omxspi = None
    if yf is not None:
        try:
            t1 = yf.Ticker("^OMXS30")
            fast1 = getattr(t1, "fast_info", {}) or {}
            omxs30 = fast1.get("last_price") or fast1.get("lastPrice")
            if omxs30 is None:
                hist1 = t1.history(period="1d")
                if not hist1.empty:
                    omxs30 = float(hist1["Close"].iloc[-1])
        except Exception:
            omxs30 = None
        try:
            t2 = yf.Ticker("^OMXSPI")
            fast2 = getattr(t2, "fast_info", {}) or {}
            omxspi = fast2.get("last_price") or fast2.get("lastPrice")
            if omxspi is None:
                hist2 = t2.history(period="1d")
                if not hist2.empty:
                    omxspi = float(hist2["Close"].iloc[-1])
        except Exception:
            omxspi = None
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            'UPDATE coresvdata SET "CoreSverige" = ?, "OMXS30" = ?, "OMXSPI" = ? WHERE DATE("Datum") = ?',
            (total_value, omxs30, omxspi, today),
        )
        if cur.rowcount == 0:
            conn.execute(
                'INSERT INTO coresvdata ("Datum", "CoreSverige", "OMXS30", "OMXSPI") VALUES (?, ?, ?, ?)',
                (today, total_value, omxs30, omxspi),
            )
        conn.commit()
    referer = request.headers.get("referer") or "/core-sverige"
    return RedirectResponse(url=referer, status_code=303)


def _upsert_model_value(table: str, column: str, value: float) -> None:
    today = datetime.now().strftime("%Y-%m-%d")
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            f'UPDATE {table} SET "{column}" = ? WHERE DATE("Datum") = ?',
            (value, today),
        )
        if cur.rowcount == 0:
            conn.execute(
                f'INSERT INTO {table} ("Datum", "{column}") VALUES (?, ?)',
                (today, value),
            )
        conn.commit()


def _fetch_yf_last(ticker: str) -> float | None:
    if yf is None:
        return None
    try:
        t = yf.Ticker(ticker)
        fast = getattr(t, "fast_info", {}) or {}
        price = fast.get("last_price") or fast.get("lastPrice")
        if price is None:
            hist = t.history(period="1d")
            if not hist.empty:
                price = float(hist["Close"].iloc[-1])
        return float(price) if price is not None else None
    except Exception:
        return None


def _fetch_yf_last_any(tickers: list[str]) -> float | None:
    for ticker in tickers:
        value = _fetch_yf_last(ticker)
        if value is not None:
            return value
    return None


def _model_total_from_actions(table: str) -> float:
    actions = _load_sheet_from_db(table)
    if actions.empty or "Värdepapper" not in actions.columns or "Antal" not in actions.columns:
        return 0.0
    actions = actions.copy()
    actions["Värdepapper"] = actions["Värdepapper"].astype(str).str.strip()
    actions["Värdepapper_norm"] = actions["Värdepapper"].str.casefold()
    actions["Antal"] = _to_float_series(actions["Antal"]).fillna(0)
    if "Datum" in actions.columns:
        actions["Datum"] = pd.to_datetime(actions["Datum"], errors="coerce")
        actions = actions.sort_values(by="Datum")
    if "Kurs" in actions.columns:
        actions["Kurs"] = _to_float_series(actions["Kurs"]).fillna(0)

    nettokassa = _latest_nettokassa(actions)

    taggar_df = _load_taggar_table()
    currency_kurs_map = {}
    if not taggar_df.empty and "Short Name" in taggar_df.columns and "Kurs" in taggar_df.columns:
        currency_map = (
            taggar_df[["Short Name", "Kurs"]]
            .dropna()
            .assign(short=lambda d: d["Short Name"].astype(str).str.strip())
        )
        currency_map = currency_map[currency_map["short"] != ""]
        currency_kurs_map = dict(
            zip(currency_map["short"], currency_map["Kurs"].apply(_to_float))
        )

    kurs_by_model = {}
    fx_by_model = {}
    if not taggar_df.empty and "Modellnamn" in taggar_df.columns:
        if "Kurs" in taggar_df.columns:
            kurs_map = (
                taggar_df[["Modellnamn", "Kurs"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            kurs_map = kurs_map[kurs_map["modell"] != ""]
            kurs_by_model = dict(
                zip(kurs_map["modell"].str.casefold(), kurs_map["Kurs"].apply(_to_float))
            )
        if "FX" in taggar_df.columns:
            fx_map = (
                taggar_df[["Modellnamn", "FX"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            fx_map = fx_map[fx_map["modell"] != ""]
            fx_by_model = dict(
                zip(fx_map["modell"].str.casefold(), fx_map["FX"])
            )

    holdings = (
        actions.groupby("Värdepapper_norm", dropna=False)["Antal"]
        .sum()
        .reset_index()
    )
    holdings = holdings[holdings["Värdepapper_norm"].astype(str).str.strip() != ""]
    holdings = holdings[holdings["Antal"] != 0]

    total_value = 0.0
    for _, row in holdings.iterrows():
        model_name = str(row.get("Värdepapper_norm", "")).strip()
        if model_name.upper() in {"KASSA", "SEK"}:
            continue
        qty = _to_float(row.get("Antal", 0)) or 0
        kurs = kurs_by_model.get(model_name, None)
        if kurs is None:
            continue
        fx_code = fx_by_model.get(model_name, "")
        fx_rate = _to_float(currency_kurs_map.get(str(fx_code).strip(), 1)) or 1
        total_value += qty * kurs * fx_rate

    if nettokassa is not None:
        total_value += float(nettokassa)
    return float(total_value)


@app.post("/edge-data-add")
async def edge_data_add(request: Request):
    form = await request.form()
    total_value = _to_float(form.get("total_value")) or 0
    today = datetime.now().strftime("%Y-%m-%d")
    firstnorth = _fetch_yf_last("^FIRSTNORTHSEK")
    omxsscpi = _fetch_yf_last("^OMXSSCPI")
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            'UPDATE edgedata SET "Edge" = ?, "FirstNorth" = ?, "OMXSSCPI" = ? WHERE DATE("Datum") = ?',
            (total_value, firstnorth, omxsscpi, today),
        )
        if cur.rowcount == 0:
            conn.execute(
                'INSERT INTO edgedata ("Datum", "Edge", "FirstNorth", "OMXSSCPI") VALUES (?, ?, ?, ?)',
                (today, total_value, firstnorth, omxsscpi),
            )
        conn.commit()
    referer = request.headers.get("referer") or "/edge"
    return RedirectResponse(url=referer, status_code=303)


@app.post("/corev-data-add")
async def corev_data_add(request: Request):
    form = await request.form()
    total_value = _to_float(form.get("total_value")) or 0
    today = datetime.now().strftime("%Y-%m-%d")
    msci_usd = _fetch_yf_last("^990100-USD-STRD")
    usdsek = _fetch_yf_last_any(["USDSEK=X", "SEK=X"])
    msci_sek = (msci_usd * usdsek) if msci_usd is not None and usdsek is not None else None
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            'UPDATE corevdata SET "CoreVärlden" = ?, "MSCI World SEK" = ? WHERE DATE("Datum") = ?',
            (total_value, msci_sek, today),
        )
        if cur.rowcount == 0:
            conn.execute(
                'INSERT INTO corevdata ("Datum", "CoreVärlden", "MSCI World SEK") VALUES (?, ?, ?)',
                (today, total_value, msci_sek),
            )
        conn.commit()
    referer = request.headers.get("referer") or "/core-varlden"
    return RedirectResponse(url=referer, status_code=303)


@app.post("/alt-data-add")
async def alt_data_add(request: Request):
    form = await request.form()
    total_value = _to_float(form.get("total_value")) or 0
    today = datetime.now().strftime("%Y-%m-%d")
    rly_usd = _fetch_yf_last("RLY")
    usdsek = _fetch_yf_last_any(["USDSEK=X", "SEK=X"])
    rly_sek = (rly_usd * usdsek) if rly_usd is not None and usdsek is not None else None
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            'UPDATE altdata SET "Alternativa" = ?, "RLY SEK" = ? WHERE DATE("Datum") = ?',
            (total_value, rly_sek, today),
        )
        if cur.rowcount == 0:
            conn.execute(
                'INSERT INTO altdata ("Datum", "Alternativa", "RLY SEK") VALUES (?, ?, ?)',
                (today, total_value, rly_sek),
            )
        conn.commit()
    referer = request.headers.get("referer") or "/alternativa"
    return RedirectResponse(url=referer, status_code=303)


@app.post("/models-update")
def models_update(request: Request):
    # Core Sverige (includes OMXS30/OMXSPI)
    coresv_total = _model_total_from_actions("coresvactions")
    today = datetime.now().strftime("%Y-%m-%d")
    omxs30 = _fetch_yf_last("^OMXS30")
    omxspi = _fetch_yf_last("^OMXSPI")
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            'UPDATE coresvdata SET "CoreSverige" = ?, "OMXS30" = ?, "OMXSPI" = ? WHERE DATE("Datum") = ?',
            (coresv_total, omxs30, omxspi, today),
        )
        if cur.rowcount == 0:
            conn.execute(
                'INSERT INTO coresvdata ("Datum", "CoreSverige", "OMXS30", "OMXSPI") VALUES (?, ?, ?, ?)',
                (today, coresv_total, omxs30, omxspi),
            )
        conn.commit()

    # Edge
    edge_total = _model_total_from_actions("edgeactions")
    firstnorth = _fetch_yf_last("^FIRSTNORTHSEK")
    omxsscpi = _fetch_yf_last("^OMXSSCPI")
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            'UPDATE edgedata SET "Edge" = ?, "FirstNorth" = ?, "OMXSSCPI" = ? WHERE DATE("Datum") = ?',
            (edge_total, firstnorth, omxsscpi, today),
        )
        if cur.rowcount == 0:
            conn.execute(
                'INSERT INTO edgedata ("Datum", "Edge", "FirstNorth", "OMXSSCPI") VALUES (?, ?, ?, ?)',
                (today, edge_total, firstnorth, omxsscpi),
            )
        conn.commit()

    # Core Världen
    corev_total = _model_total_from_actions("corevactions")
    msci_usd = _fetch_yf_last("^990100-USD-STRD")
    usdsek = _fetch_yf_last_any(["USDSEK=X", "SEK=X"])
    msci_sek = (msci_usd * usdsek) if msci_usd is not None and usdsek is not None else None
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            'UPDATE corevdata SET "CoreVärlden" = ?, "MSCI World SEK" = ? WHERE DATE("Datum") = ?',
            (corev_total, msci_sek, today),
        )
        if cur.rowcount == 0:
            conn.execute(
                'INSERT INTO corevdata ("Datum", "CoreVärlden", "MSCI World SEK") VALUES (?, ?, ?)',
                (today, corev_total, msci_sek),
            )
        conn.commit()

    # Alternativa
    alt_total = _model_total_from_actions("altactions")
    rly_usd = _fetch_yf_last("RLY")
    usdsek = _fetch_yf_last_any(["USDSEK=X", "SEK=X"])
    rly_sek = (rly_usd * usdsek) if rly_usd is not None and usdsek is not None else None
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            'UPDATE altdata SET "Alternativa" = ?, "RLY SEK" = ? WHERE DATE("Datum") = ?',
            (alt_total, rly_sek, today),
        )
        if cur.rowcount == 0:
            conn.execute(
                'INSERT INTO altdata ("Datum", "Alternativa", "RLY SEK") VALUES (?, ?, ?)',
                (today, alt_total, rly_sek),
            )
        conn.commit()

    referer = request.headers.get("referer", "/")
    return RedirectResponse(referer, status_code=303)


@app.post("/strategi-update")
async def strategi_update(request: Request):
    form = await request.form()
    data = {}
    values = []
    valid = True
    for key, value in form.items():
        if key.startswith("key_"):
            idx = key.split("_", 1)[1]
            label = value
            val = form.get(f"val_{idx}", "")
            num = _to_float(val)
            if num is None:
                valid = False
            else:
                values.append(num)
            data[label] = val

    total = sum(values) if values else 0
    if not valid or abs(total - 1) > 0.001:
        referer = request.headers.get("referer", "/ombalansering")
        return RedirectResponse(referer, status_code=303)

    if data:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame([data])
        with sqlite3.connect(DB_PATH) as conn:
            df.to_sql("strategi", conn, if_exists="replace", index=False)
        # Recompute dynamic allocations for all Mandat rows using new strategy values
        mandat_df = _load_mandat_table()
        if not mandat_df.empty:
            number_col = "Number" if "Number" in mandat_df.columns else "Nummer"
            flags_df = _load_mandat_flags()
            if not flags_df.empty and number_col in mandat_df.columns and "number" in flags_df.columns:
                flags_df = flags_df.rename(
                    columns={
                        "number": number_col,
                        "dynamisk": "dynamisk",
                        "coresv": "coresv",
                        "coreva": "corevä",
                        "edge": "edge",
                        "alts": "alts",
                    }
                )
                flags_df[number_col] = flags_df[number_col].astype(str).str.strip()
                mandat_df[number_col] = mandat_df[number_col].astype(str).str.strip()
                mandat_df = mandat_df.merge(flags_df, on=number_col, how="left", suffixes=("", "_flag"))
                for col in ["dynamisk", "coresv", "corevä", "edge", "alts"]:
                    flag_col = f"{col}_flag"
                    if flag_col in mandat_df.columns:
                        if col in mandat_df.columns:
                            mandat_df[col] = mandat_df[flag_col].fillna(mandat_df[col])
                        else:
                            mandat_df[col] = mandat_df[flag_col]
                        mandat_df.drop(columns=[flag_col], inplace=True)
                if "coreva" in mandat_df.columns and "corevä" not in mandat_df.columns:
                    mandat_df = mandat_df.rename(columns={"coreva": "corevä"})

            for col in ["dynamisk", "coresv", "corevä", "edge", "alts"]:
                if col not in mandat_df.columns:
                    mandat_df[col] = 0

            strategi_vals = {
                "coresv": _to_float(data.get("Core Sverige", 0)) or 0,
                "corevä": _to_float(data.get("Core Världen", 0)) or 0,
                "edge": _to_float(data.get("Edge", 0)) or 0,
                "alts": _to_float(data.get("Alternativa", 0)) or 0,
            }

            coresv_series = pd.to_numeric(mandat_df["coresv"], errors="coerce").fillna(0)
            coreva_series = pd.to_numeric(mandat_df["corevä"], errors="coerce").fillna(0)
            edge_series = pd.to_numeric(mandat_df["edge"], errors="coerce").fillna(0)
            alts_series = pd.to_numeric(mandat_df["alts"], errors="coerce").fillna(0)
            flags_sum = (
                strategi_vals.get("coresv", 0) * coresv_series
                + strategi_vals.get("corevä", 0) * coreva_series
                + strategi_vals.get("edge", 0) * edge_series
                + strategi_vals.get("alts", 0) * alts_series
            )
            fi_value = pd.to_numeric(mandat_df["FI"], errors="coerce").fillna(0) if "FI" in mandat_df.columns else 0
            scale = 1 - fi_value
            denom = flags_sum.replace(0, np.nan)

            mandat_df["dynCS"] = (
                (mandat_df["coresv"].astype(float).where(mandat_df["coresv"] == 1, 0) * strategi_vals.get("coresv", 0))
                / denom
                * scale
            )
            mandat_df["dynCV"] = (
                (mandat_df["corevä"].astype(float).where(mandat_df["corevä"] == 1, 0) * strategi_vals.get("corevä", 0))
                / denom
                * scale
            )
            mandat_df["dynEd"] = (
                (mandat_df["edge"].astype(float).where(mandat_df["edge"] == 1, 0) * strategi_vals.get("edge", 0))
                / denom
                * scale
            )
            mandat_df["dynAlt"] = (
                (mandat_df["alts"].astype(float).where(mandat_df["alts"] == 1, 0) * strategi_vals.get("alts", 0))
                / denom
                * scale
            )

            mandat_df.loc[mandat_df["dynamisk"] != 1, ["dynCS", "dynCV", "dynEd", "dynAlt"]] = 0.0
            for col in ["dynCS", "dynCV", "dynEd", "dynAlt"]:
                mandat_df[col] = pd.to_numeric(mandat_df[col], errors="coerce").fillna(0)

            dyn_rows = []
            if number_col in mandat_df.columns:
                for _, row in mandat_df.iterrows():
                    number_val = str(row.get(number_col, "")).strip()
                    if not number_val:
                        continue
                    dyn_rows.append(
                        {
                            "number": number_val,
                            "dynCS": row.get("dynCS", 0),
                            "dynCV": row.get("dynCV", 0),
                            "dynEd": row.get("dynEd", 0),
                            "dynAlt": row.get("dynAlt", 0),
                        }
                    )
            _save_mandat_dyn(dyn_rows)

    referer = request.headers.get("referer", "/ombalansering")
    return RedirectResponse(referer, status_code=303)


@app.post("/import")
async def import_excel(request: Request, excel_file: UploadFile = File(default=None)):
    uploaded_content: bytes | None = None
    if excel_file is not None and excel_file.filename:
        uploaded_content = await excel_file.read()
    _import_excel_to_db(uploaded_content)
    referer = request.headers.get("referer", "/")
    return RedirectResponse(referer, status_code=303)


@app.post("/import-db")
async def import_db_excel(request: Request, db_excel_file: UploadFile = File(default=None)):
    if db_excel_file is None or not db_excel_file.filename:
        referer = request.headers.get("referer", "/")
        return RedirectResponse(referer, status_code=303)
    uploaded_content = await db_excel_file.read()
    if not uploaded_content:
        referer = request.headers.get("referer", "/")
        return RedirectResponse(referer, status_code=303)
    _import_db_excel_to_db(uploaded_content)
    referer = request.headers.get("referer", "/")
    return RedirectResponse(referer, status_code=303)


def _normalize_number(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()


def _normalize_key(value: str) -> str:
    return str(value).strip().lower()


def _normalize_holding_name(value: str) -> str:
    return " ".join(str(value).replace("\u00a0", " ").strip().lower().split())

def _norm_label(value: str) -> str:
    if value is None:
        return ""
    normalized = str(value).replace("?", "a")
    normalized = unicodedata.normalize("NFKD", normalized)
    return "".join(ch for ch in normalized if ch.isalnum()).lower()


def _ensure_label(view: pd.DataFrame, target_label: str, candidates: list[str]) -> None:
    if target_label in view.columns:
        return
    target_norm = _norm_label(target_label)
    for col in view.columns:
        if _norm_label(col) == target_norm:
            view[target_label] = view[col]
            return
    for col in view.columns:
        if any(_norm_label(col) == _norm_label(c) for c in candidates):
            view[target_label] = view[col]
            return



def _safe_rows(df: pd.DataFrame) -> list[dict]:
    clean = df.copy()
    clean = clean.where(pd.notna(clean), "")
    return clean.to_dict(orient="records")


def _format_number(value: float, decimals: int) -> str:
    fmt = f"{value:,.{decimals}f}"
    return fmt.replace(",", " ").replace(".", ",")


def _to_float_series(series: pd.Series) -> pd.Series:
    return series.apply(lambda v: _to_float(v) if v is not None else None)


def _parse_date_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", format="%Y-%m-%d")
    return parsed.fillna(pd.to_datetime(series, errors="coerce"))


def _model_weights_for_modul(modul_label: str, taggar_df: pd.DataFrame) -> dict[str, float]:
    table_map = {
        "core sverige": "coresvactions",
        "edge": "edgeactions",
        "alternativa": "altactions",
        "core världen": "corevactions",
    }
    table = table_map.get(str(modul_label).strip().lower())
    if not table:
        return {}
    actions = _load_sheet_from_db(table)
    if actions.empty or "Värdepapper" not in actions.columns or "Antal" not in actions.columns:
        return {}
    actions = actions.copy()
    actions["Värdepapper"] = actions["Värdepapper"].astype(str).str.strip()
    actions["Värdepapper_norm"] = actions["Värdepapper"].str.casefold()
    actions["Antal"] = _to_float_series(actions["Antal"]).fillna(0)
    if "Kurs" in actions.columns:
        actions["Kurs"] = _to_float_series(actions["Kurs"]).fillna(0)
    holdings = (
        actions.groupby("Värdepapper_norm", dropna=False)["Antal"]
        .sum()
        .reset_index()
    )
    holdings = holdings[holdings["Värdepapper_norm"].astype(str).str.strip() != ""]
    holdings = holdings[holdings["Antal"] != 0]

    # taggar maps
    kurs_by_model = {}
    fx_by_model = {}
    modell_to_short = {}
    currency_kurs_map = {}
    if not taggar_df.empty:
        if "Modellnamn" in taggar_df.columns and "Kurs" in taggar_df.columns:
            kurs_map = (
                taggar_df[["Modellnamn", "Kurs"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            kurs_map = kurs_map[kurs_map["modell"] != ""]
            kurs_by_model = dict(
                zip(kurs_map["modell"].str.casefold(), kurs_map["Kurs"].apply(_to_float))
            )
        if "Modellnamn" in taggar_df.columns and "FX" in taggar_df.columns:
            fx_map = (
                taggar_df[["Modellnamn", "FX"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            fx_map = fx_map[fx_map["modell"] != ""]
            fx_by_model = dict(
                zip(fx_map["modell"].str.casefold(), fx_map["FX"])
            )
        if "Modellnamn" in taggar_df.columns and "Short Name" in taggar_df.columns:
            model_map = (
                taggar_df[["Modellnamn", "Short Name"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip(),
                        short=lambda d: d["Short Name"].astype(str).str.strip())
            )
            model_map = model_map[(model_map["modell"] != "") & (model_map["short"] != "")]
            modell_to_short = dict(
                zip(model_map["modell"].str.casefold(), model_map["short"])
            )
        if "Short Name" in taggar_df.columns and "Kurs" in taggar_df.columns:
            currency_map = (
                taggar_df[["Short Name", "Kurs"]]
                .dropna()
                .assign(short=lambda d: d["Short Name"].astype(str).str.strip())
            )
            currency_map = currency_map[currency_map["short"] != ""]
            currency_kurs_map = dict(
                zip(currency_map["short"], currency_map["Kurs"].apply(_to_float))
            )

    values_by_short = {}
    total_value = 0.0
    for _, row in holdings.iterrows():
        model_name = str(row.get("Värdepapper_norm", "")).strip()
        if model_name.upper() in {"KASSA", "SEK"}:
            continue
        qty = _to_float(row.get("Antal", 0)) or 0
        kurs = kurs_by_model.get(model_name, None)
        if kurs is None:
            value = 0
        else:
            fx_code = fx_by_model.get(model_name, "")
            fx_rate = _to_float(currency_kurs_map.get(str(fx_code).strip(), 1)) or 1
            value = qty * kurs * fx_rate
        short_name = modell_to_short.get(model_name, model_name)
        short_norm = _normalize_holding_name(short_name)
        values_by_short[short_norm] = values_by_short.get(short_norm, 0) + value
        total_value += value
    if total_value <= 0:
        return {}
    return {k: v / total_value for k, v in values_by_short.items()}


def _latest_nettokassa(actions: pd.DataFrame) -> float | None:
    if actions is None or actions.empty or "Nettokassa" not in actions.columns:
        return None
    temp = actions[["Nettokassa"]].copy()
    temp["Nettokassa"] = _to_float_series(temp["Nettokassa"])
    if "Datum" in actions.columns:
        temp["Datum"] = pd.to_datetime(actions["Datum"], errors="coerce")
        temp = temp.sort_values(by="Datum")
    temp = temp[temp["Nettokassa"].notna()]
    if temp.empty:
        return None
    return float(temp["Nettokassa"].iloc[-1])


def _append_model_action(table: str, payload: dict) -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = _load_sheet_from_db(table)
    required_cols = ["Datum", "Värdepapper", "Transaktionstyp", "Antal", "Kurs", "Kassaflöde", "Nettokassa"]
    if df.empty and len(df.columns) == 0:
        df = pd.DataFrame(columns=required_cols)
    else:
        for col in required_cols:
            if col not in df.columns:
                df[col] = pd.NA
    if "row_id" not in df.columns:
        df.insert(0, "row_id", range(1, len(df) + 1))
    df["row_id"] = pd.to_numeric(df["row_id"], errors="coerce")

    antal = _to_float(payload.get("Antal")) or 0
    kurs = _to_float(payload.get("Kurs")) or 0
    kassaflode = antal * kurs * -1

    prev_nettokassa = 0.0
    if not df.empty and "Nettokassa" in df.columns:
        temp = df.copy()
        if "Datum" in temp.columns:
            temp["__date"] = _parse_date_series(temp["Datum"])
            temp = temp.sort_values(by="__date")
        temp["Nettokassa"] = _to_float_series(temp["Nettokassa"])
        temp = temp[temp["Nettokassa"].notna()]
        if not temp.empty:
            prev_nettokassa = float(temp["Nettokassa"].iloc[-1])

    nettokassa = prev_nettokassa + kassaflode
    next_id = int(df["row_id"].max() or 0) + 1
    row = {
        "row_id": next_id,
        "Datum": payload.get("Datum", ""),
        "Värdepapper": payload.get("Värdepapper", ""),
        "Transaktionstyp": payload.get("Transaktionstyp", ""),
        "Antal": antal,
        "Kurs": kurs,
        "Kassaflöde": kassaflode,
        "Nettokassa": nettokassa,
    }
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(table, conn, if_exists="replace", index=False)


def _ensure_row_id(df: pd.DataFrame, table: str) -> pd.DataFrame:
    if df.empty:
        return df
    if "row_id" in df.columns and df["row_id"].notna().all():
        return df
    df = df.copy()
    if "row_id" not in df.columns:
        df.insert(0, "row_id", range(1, len(df) + 1))
    else:
        df["row_id"] = pd.to_numeric(df["row_id"], errors="coerce")
        mask = df["row_id"].isna()
        if mask.any():
            next_id = int(df["row_id"].max() or 0) + 1
            for idx in df[mask].index:
                df.at[idx, "row_id"] = next_id
                next_id += 1
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(table, conn, if_exists="replace", index=False)
    return df


def _recalc_kassa(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "Antal" in df.columns:
        df["Antal"] = _to_float_series(df["Antal"]).fillna(0)
    if "Kurs" in df.columns:
        df["Kurs"] = _to_float_series(df["Kurs"]).fillna(0)
    df["Kassaflöde"] = (df["Antal"] * df["Kurs"] * -1).astype(float)
    if "Datum" in df.columns:
        df["__sort"] = _parse_date_series(df["Datum"])
    else:
        df["__sort"] = range(len(df))
    df_sorted = df.sort_values(by="__sort")
    nettokassa = []
    running = 0.0
    for _, row in df_sorted.iterrows():
        running += _to_float(row.get("Kassaflöde", 0)) or 0
        nettokassa.append(running)
    df_sorted["Nettokassa"] = nettokassa
    df.loc[df_sorted.index, "Nettokassa"] = df_sorted["Nettokassa"]
    df.drop(columns=["__sort"], inplace=True, errors="ignore")
    return df


def _recalc_kassa_from_date(df: pd.DataFrame, start_date) -> pd.DataFrame:
    df = df.copy()
    if "Antal" in df.columns:
        df["Antal"] = _to_float_series(df["Antal"]).fillna(0)
    if "Kurs" in df.columns:
        df["Kurs"] = _to_float_series(df["Kurs"]).fillna(0)
    df["Kassaflöde"] = (df["Antal"] * df["Kurs"] * -1).astype(float)
    if "Datum" in df.columns:
        df["__sort"] = _parse_date_series(df["Datum"])
    else:
        df["__sort"] = range(len(df))
    df_sorted = df.sort_values(by="__sort")

    # find start index
    start_mask = df_sorted["__sort"] >= pd.to_datetime(start_date, errors="coerce")
    if not start_mask.any():
        df.drop(columns=["__sort"], inplace=True, errors="ignore")
        return df

    start_idx = df_sorted.index[start_mask][0]
    before = df_sorted.loc[:start_idx].iloc[:-1]
    base = 0.0
    if not before.empty and "Nettokassa" in before.columns:
        base_val = _to_float(before["Nettokassa"].iloc[-1])
        base = base_val if base_val is not None else 0.0

    running = base
    for idx in df_sorted.loc[start_idx:].index:
        running += _to_float(df_sorted.at[idx, "Kassaflöde"]) or 0
        df_sorted.at[idx, "Nettokassa"] = running

    df.loc[df_sorted.index, "Nettokassa"] = df_sorted["Nettokassa"]
    df.drop(columns=["__sort"], inplace=True, errors="ignore")
    return df


def _to_float(value) -> float | None:
    try:
        if isinstance(value, str):
            cleaned = value.strip().replace(" ", "")
            is_percent = "%" in cleaned
            cleaned = cleaned.replace("%", "")
            cleaned = cleaned.replace(",", ".")
            num = float(cleaned)
            return num / 100 if is_percent else num
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_number_value(value) -> str:
    text = str(value).strip()
    try:
        num = float(text)
        if num.is_integer():
            return str(int(num))
    except (TypeError, ValueError):
        pass
    return text


def _coerce_cell_for_column(df: pd.DataFrame, col: str, value):
    if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
        num = _to_float(value)
        return num if num is not None else pd.NA
    return value


def format_cell(column: str, value) -> str:
    if value is None or value == "":
        return ""
    col = column.strip().lower()
    if col in {"godkännande", "datum", "date"}:
        try:
            dt = pd.to_datetime(value, errors="coerce")
            if pd.isna(dt):
                return ""
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value)

    if col == "number":
        return str(int(num)) if num.is_integer() else str(num)
    if num == 0:
        return ""
    if col == "fi":
        return f"{_format_number(num * 100, 0)}%"
    if col in {"alt", "cs", "cv", "ed", "dyncs", "dyncv", "dyned", "dynalt"}:
        return f"{_format_number(num * 100, 0)}%"
    if col in {"fi % modell", "fi % portfölj"}:
        return _format_number(num * 100, 2) + "%"
    if col == "att köpa":
        return _format_number(num, 1)
    if col in {"gav", "utv.", "kurs"}:
        return _format_number(num * 100, 1) + "%" if col == "utv." else _format_number(num, 1)
    if "price" in col:
        return _format_number(num, 2)
    return _format_number(num, 0)


def format_percent(value) -> str:
    if value is None or value == "":
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""
    return f"{_format_number(num * 100, 0)}%"


def format_percent_1(value) -> str:
    if value is None or value == "":
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""
    return f"{_format_number(num * 100, 1)}%"


@app.get("/", response_class=HTMLResponse)
def index(request: Request, q: str = ""):
    search_query = q.strip()
    number = search_query
    results = {}
    dashboard = None
    allocation = None
    holdings_total = None
    alternativa_total = None
    alternativa_share = None
    core_sverige_share = None
    core_varlden_share = None
    edge_share = None
    fixed_income_share = None
    ovrigt_share = None
    fixed_income_model = None
    valuta_total = None
    total_share = None
    valuta_share = None
    modul_totals = {}
    tillgang_totals = {}
    post_by_modul = {}
    related_numbers = []
    number_suggestions = []
    taggar_map = {}
    currency_map = {}
    model_modul_counts = {}
    model_by_modul = {}
    portfolio_modul_counts = {}
    missing_by_modul = {}
    has_holdings = False
    dyn_df = _load_mandat_dyn()
    value_series = None
    flags_df = _load_mandat_flags()
    matched_numbers: list[str] = []

    mandat_lookup = _load_sheet("Mandat")
    lookup_number_col = "Number" if "Number" in mandat_lookup.columns else "Nummer"
    if lookup_number_col in mandat_lookup.columns:
        number_suggestions = (
            mandat_lookup[lookup_number_col]
            .dropna()
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )
        number_suggestions = sorted(
            number_suggestions,
            key=lambda v: (
                float(v)
                if str(v).replace(".", "", 1).isdigit()
                else float("inf"),
                str(v),
            ),
        )
        if search_query:
            exact = (
                mandat_lookup[lookup_number_col]
                .dropna()
                .astype(str)
                .str.strip()
            )
            if (exact == search_query).any():
                matched_numbers = [search_query]
            elif "Kund" in mandat_lookup.columns:
                kund_mask = (
                    mandat_lookup["Kund"]
                    .fillna("")
                    .astype(str)
                    .str.casefold()
                    .str.contains(search_query.casefold(), na=False)
                )
                if kund_mask.any():
                    matched_numbers = (
                        mandat_lookup.loc[kund_mask, lookup_number_col]
                        .dropna()
                        .astype(str)
                        .str.strip()
                        .unique()
                        .tolist()
                    )
                    matched_numbers = sorted(
                        matched_numbers,
                        key=lambda v: (
                            float(v)
                            if str(v).replace(".", "", 1).isdigit()
                            else float("inf"),
                            str(v),
                        ),
                    )
            if matched_numbers:
                number = matched_numbers[0]
                matched_numbers = [number]

    if not number:
        for sheet in DISPLAY_SHEETS:
            if sheet in DISPLAY_MAP:
                display_cols = [label for _, label in DISPLAY_MAP[sheet]]
                results[sheet] = {
                    "title": "Holdings" if sheet == "Detaljerat" else sheet,
                    "columns": display_cols,
                    "rows": [],
                }
            else:
                results[sheet] = {"title": sheet, "columns": [], "rows": []}
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "q": q,
                "results": results,
                "display_sheets": DISPLAY_SHEETS,
                "dashboard": dashboard,
                "allocation": allocation,
                "holdings_total": holdings_total,
                "alternativa_total": alternativa_total,
                "alternativa_share": alternativa_share,
                "core_sverige_share": core_sverige_share,
                "core_varlden_share": core_varlden_share,
                "edge_share": edge_share,
                "fixed_income_share": fixed_income_share,
                "ovrigt_share": ovrigt_share,
                "fixed_income_model": fixed_income_model,
                "valuta_total": valuta_total,
                "total_share": total_share,
                "valuta_share": valuta_share,
                "modul_totals": modul_totals,
                "tillgang_totals": tillgang_totals,
                "post_by_modul": post_by_modul,
                "portfolio_modul_counts": portfolio_modul_counts,
                "model_modul_counts": model_modul_counts,
                "missing_by_modul": missing_by_modul,
                "has_holdings": has_holdings,
                "related_numbers": related_numbers,
                "number_suggestions": number_suggestions,
                "format_cell": format_cell,
                "format_percent": format_percent,
            },
        )

    try:
        taggar_df = _load_sheet("Taggar")
        if "Short Name" in taggar_df.columns:
            taggar_map = {}
            for _, row in taggar_df.iterrows():
                key = _normalize_key(row.get("Short Name", ""))
                if not key:
                    continue
                taggar_map[key] = row.to_dict()
            if "Kurs" in taggar_df.columns:
                for _, row in taggar_df.iterrows():
                    key = _normalize_key(row.get("Short Name", ""))
                    if not key:
                        continue
                    kurs = pd.to_numeric(row.get("Kurs", None), errors="coerce")
                    if pd.notna(kurs):
                        currency_map[key] = float(kurs)
            if "Modul" in taggar_df.columns:
                counts = (
                    taggar_df["Modul"]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .value_counts()
                )
                model_modul_counts = counts.to_dict()
                taggar_modul_map = (
                    taggar_df[["Short Name", "Modul"]]
                    .dropna()
                    .assign(
                        short_norm=taggar_df["Short Name"].apply(_normalize_key),
                        modul_norm=taggar_df["Modul"].astype(str).str.strip().str.lower(),
                    )
                )
                model_by_modul = {}
                for _, row in taggar_modul_map.iterrows():
                    m = row["modul_norm"]
                    s = row["short_norm"]
                    model_by_modul.setdefault(m, set()).add(s)
    except Exception:
        taggar_map = {}
        currency_map = {}
        model_modul_counts = {}
        model_by_modul = {}

    for sheet in SHEETS:
        df = _load_sheet(sheet)
        number_col = None
        for col in df.columns:
            col_key = col.strip().lower()
            if col_key in {"number", "nummer"}:
                number_col = col
                break

        if number_col is None:
            results[sheet] = {"columns": df.columns.tolist(), "rows": []}
            continue

        if sheet == "Mandat":
            if not flags_df.empty and "number" in flags_df.columns:
                flags_merge = flags_df.rename(columns={"number": number_col}).copy()
                flags_merge[number_col] = flags_merge[number_col].astype(str).str.strip()
                df[number_col] = df[number_col].astype(str).str.strip()
                df = df.merge(flags_merge, on=number_col, how="left", suffixes=("", "_flag"))
                for col in ["dynamisk", "coresv", "coreva", "edge", "alts"]:
                    flag_col = f"{col}_flag"
                    if flag_col in df.columns:
                        if col in df.columns:
                            df[col] = df[flag_col].fillna(df[col])
                        else:
                            df[col] = df[flag_col]
                        df.drop(columns=[flag_col], inplace=True)
                if "coreva" in df.columns and "corevä" not in df.columns:
                    df = df.rename(columns={"coreva": "corevä"})
            if not dyn_df.empty and "number" in dyn_df.columns:
                dyn_merge = dyn_df.rename(columns={"number": number_col}).copy()
                dyn_merge[number_col] = dyn_merge[number_col].astype(str).str.strip()
                df[number_col] = df[number_col].astype(str).str.strip()
                df = df.merge(dyn_merge, on=number_col, how="left", suffixes=("", "_dyn"))
                for col in ["dynCS", "dynCV", "dynEd", "dynAlt"]:
                    dyn_col = f"{col}_dyn"
                    if dyn_col in df.columns:
                        if col in df.columns:
                            df[col] = df[dyn_col].fillna(df[col])
                        else:
                            df[col] = df[dyn_col].fillna(0)
                        df.drop(columns=[dyn_col], inplace=True)
            for col in ["dynamisk", "coresv", "corevä", "edge", "alts", "dynCS", "dynCV", "dynEd", "dynAlt"]:
                if col not in df.columns:
                    df[col] = 0

        if number:
            mask = _normalize_number(df[number_col]) == number
            filtered = df[mask]
        else:
            filtered = df.head(0)

        display_name = "Holdings" if sheet == "Detaljerat" else sheet

        if sheet in DISPLAY_MAP:
            mapped = DISPLAY_MAP[sheet]
            sources = [src for src, _ in mapped if src in filtered.columns]
            view = filtered[sources].copy() if sources else filtered.head(0).copy()
            if sheet == "Detaljerat" and "Short Name" in filtered.columns:
                view["Modul"] = filtered["Short Name"].apply(
                    lambda s: taggar_map.get(_normalize_key(s), {}).get("Modul", "")
                )
                view["Modul"] = (
                    view["Modul"].astype(str).str.strip().replace({"": "Övrigt", "nan": "Övrigt"})
                )
                view["RG"] = filtered["Short Name"].apply(
                    lambda s: taggar_map.get(_normalize_key(s), {}).get("RG", "")
                )
                view["Tillgångsslag"] = filtered["Short Name"].apply(
                    lambda s: taggar_map.get(_normalize_key(s), {}).get("Tillgångsslag", "")
                )
                if "Instrument Type" in filtered.columns:
                    type_map = (
                        filtered["Instrument Type"]
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .map({"share": "Aktier", "bond": "Ränta", "fund": "Aktier", "etf": "Aktier"})
                        .fillna("")
                    )
                    view["Tillgångsslag"] = view["Tillgångsslag"].where(
                        view["Tillgångsslag"].astype(str).str.strip() != "", type_map
                    )
                if "Available Count" in filtered.columns and "Price" in filtered.columns:
                    counts = _to_float_series(filtered["Available Count"]).fillna(0)
                    prices = _to_float_series(filtered["Price"]).fillna(0)
                    base_value = counts * prices
                    modul = view["Modul"].astype(str).str.strip().str.lower()
                    base_value = base_value.where(modul != "fixed income", base_value / 100)

                    if "Currency" in filtered.columns:
                        rates = filtered["Currency"].apply(
                            lambda c: currency_map.get(_normalize_key(c), 1.0)
                        )
                        base_value = base_value * _to_float_series(rates).fillna(1.0)
                    value_series = base_value.round(2)
                    view["Värde i SEK"] = value_series
                    view["Värde (sek)"] = value_series
                    _ensure_label(view, "Tillgångsslag", ["Tillgangsslag", "Tillg?ngsslag"])
                    _ensure_label(view, "Värde (sek)", ["Värde i SEK", "Varde i SEK", "V?rde i SEK"])
                    if "V?rde (sek)" in view.columns and "Värde (sek)" not in view.columns:
                        view["Värde (sek)"] = view["V?rde (sek)"]
                    if "V?rde i SEK" in view.columns and "Värde i SEK" not in view.columns:
                        view["Värde i SEK"] = view["V?rde i SEK"]
                    if "Tillg?ngsslag" in view.columns and "Tillgångsslag" not in view.columns:
                        view["Tillgångsslag"] = view["Tillg?ngsslag"]
                    holdings_total = base_value.sum(skipna=True)
                    has_holdings = bool(holdings_total and holdings_total != 0)
                    tillgang = view["Tillgångsslag"].astype(str).str.strip().str.lower()
                    valuta_total = base_value.where(tillgang == "valuta").sum(skipna=True)
                    alternativa_total = base_value.where(modul == "alternativa").sum(skipna=True)
                    core_sverige_total = base_value.where(modul == "core sverige").sum(skipna=True)
                    core_varlden_total = base_value.where(modul == "core världen").sum(skipna=True)
                    edge_total = base_value.where(modul == "edge").sum(skipna=True)
                    fixed_income_total = base_value.where(modul == "fixed income").sum(skipna=True)
                    ovrigt_total = base_value.where(modul == "övrigt").sum(skipna=True)
                    modul_totals = (
                        base_value.groupby(modul)
                        .sum(min_count=1)
                        .dropna()
                        .to_dict()
                    )
                    tillgang_totals = (
                        base_value.groupby(tillgang)
                        .sum(min_count=1)
                        .dropna()
                        .to_dict()
                    )
                    if holdings_total and holdings_total != 0:
                        alternativa_share = alternativa_total / holdings_total
                        core_sverige_share = core_sverige_total / holdings_total
                        core_varlden_share = core_varlden_total / holdings_total
                        edge_share = edge_total / holdings_total
                        fixed_income_share = fixed_income_total / holdings_total
                        ovrigt_share = ovrigt_total / holdings_total
                        total_share = 1.0
                        valuta_share = (
                            valuta_total / holdings_total if valuta_total is not None else None
                        )
                        model_sum = sum(
                            v for v in [
                                _to_float(allocation.get("Alt", "")) if allocation else None,
                                _to_float(allocation.get("CS", "")) if allocation else None,
                                _to_float(allocation.get("CV", "")) if allocation else None,
                                _to_float(allocation.get("Ed", "")) if allocation else None,
                                0.0,
                            ]
                            if v is not None
                        )
                        portfolio_sum = sum(
                            v for v in [
                                alternativa_share,
                                core_sverige_share,
                                core_varlden_share,
                                edge_share,
                                ovrigt_share,
                            ]
                            if v is not None
                        )
                        fixed_income_model = max(0, 1 - max(model_sum, portfolio_sum))
                    model_values = {
                        "alternativa": _to_float(allocation.get("Alt", "")) if allocation else None,
                        "core sverige": _to_float(allocation.get("CS", "")) if allocation else None,
                        "core världen": _to_float(allocation.get("CV", "")) if allocation else None,
                        "edge": _to_float(allocation.get("Ed", "")) if allocation else None,
                    }
                    post_by_modul = {}
                    hardcoded_counts = {
                        "core sverige": 15,
                        "edge": 15,
                        "core världen": 15,
                        "alternativa": 5,
                    }
                    for m, model_value in model_values.items():
                        model_count = hardcoded_counts.get(m, 0)
                        if holdings_total and model_value is not None and model_value > 0 and model_count:
                            post_by_modul[m] = holdings_total * model_value / model_count
                        else:
                            post_by_modul[m] = 0
                    portfolio_modul_counts = (
                        view["Modul"]
                        .dropna()
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .value_counts()
                        .to_dict()
                    )
                    fi_count = portfolio_modul_counts.get("fixed income", 0)
                    if fixed_income_total is not None and fi_count:
                        position_value = fixed_income_total / fi_count
                        post_by_modul["fixed income"] = max(50000, round(position_value / 50000) * 50000)
                    else:
                        post_by_modul["fixed income"] = 0
                    portfolio_by_modul = {}
                    if "Short Name" in filtered.columns and "Modul" in view.columns:
                        for short, modul_value in zip(filtered["Short Name"], view["Modul"]):
                            m = _normalize_key(modul_value)
                            s = _normalize_key(short)
                            if not m or not s:
                                continue
                            portfolio_by_modul.setdefault(m, set()).add(s)
                missing_by_modul = {}
                model_values = {
                    "alternativa": allocation.get("Alt", "") if allocation else "",
                    "core sverige": allocation.get("CS", "") if allocation else "",
                    "core världen": allocation.get("CV", "") if allocation else "",
                    "edge": allocation.get("Ed", "") if allocation else "",
                }
                model_by_modul = locals().get("model_by_modul", {})
                for m, model_set in model_by_modul.items():
                    model_value = model_values.get(m)
                    if model_value is None or model_value == "" or float(model_value) <= 0:
                        continue
                        missing = sorted(model_set - portfolio_by_modul.get(m, set()))
                        if missing:
                            missing_by_modul[m] = missing
                else:
                    view["Värde i SEK"] = ""
            for src, label in mapped:
                if src not in view.columns and label not in view.columns:
                    view[label] = ""
            view = view.rename(columns={src: label for src, label in mapped if src in view.columns})
            view = view.loc[:, ~view.columns.duplicated()]
            if sheet == "Detaljerat" and value_series is not None:
                view["Värde (sek)"] = value_series.values
            if sheet == "Detaljerat" and "Modul" in view.columns:
                view = view.sort_values(by="Modul", kind="stable")
            view = view[[label for _, label in mapped]]
            rows = _safe_rows(view)
            if sheet == "Detaljerat" and "Modul" in view.columns and "Värde (sek)" in view.columns:
                rows = []
                for modul_name, group in view.groupby("Modul", sort=False):
                    spacer_row = {col: "" for col in view.columns}
                    spacer_row["_row_class"] = "spacer-row"
                    rows.append(spacer_row)
                    total_value = pd.to_numeric(group["Värde (sek)"], errors="coerce").sum(skipna=True)
                    share = (total_value / holdings_total) if holdings_total else None
                    total_text = _format_number(total_value, 0)
                    share_text = f"{_format_number(share * 100, 0)}%" if share is not None else ""
                    subtotal_row = {col: "" for col in view.columns}
                    subtotal_row["Innehav"] = f"{modul_name}"
                    subtotal_row["Modul"] = ""
                    subtotal_row["Värde (sek)"] = f"{total_text} ({share_text})" if share_text else total_text
                    subtotal_row["_row_class"] = "subtotal-row"
                    rows.append(subtotal_row)
                    rows.extend(_safe_rows(group))

            results[sheet] = {
                "title": display_name,
                "columns": view.columns.tolist(),
                "rows": rows,
            }
        else:
            results[sheet] = {
                "title": display_name,
                "columns": df.columns.tolist(),
                "rows": _safe_rows(filtered),
            }

        if sheet == "Mandat":
            if number_col:
                number_suggestions = (
                    df[number_col]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .unique()
                    .tolist()
                )
                number_suggestions = sorted(
                    number_suggestions,
                    key=lambda v: (float(v) if str(v).replace(".", "", 1).isdigit() else float("inf"), str(v)),
                )
            if number and not filtered.empty and dashboard is None:
                row = filtered.iloc[0].where(pd.notna(filtered.iloc[0]), "")
                dashboard = {
                    "Number": row.get("Number", row.get("Nummer", "")),
                    "Kund": row.get("Kund", ""),
                    "Mandat": row.get("Mandat", ""),
                    "Rådgivare": row.get("Rådgivare", ""),
                    "Mandatnotering": row.get("Mandatnotering", ""),
                    "Förvaltningsnotering": row.get("Förvaltningsnotering", ""),
                    "FI-notering": row.get("FI-notering", ""),
                }
                use_dyn = _to_float(row.get("dynamisk", 0)) == 1
                allocation = {
                    "Alt": row.get("dynAlt" if use_dyn else "Alt", ""),
                    "CS": row.get("dynCS" if use_dyn else "CS", ""),
                    "CV": row.get("dynCV" if use_dyn else "CV", ""),
                    "Ed": row.get("dynEd" if use_dyn else "Ed", ""),
                }
                kund_value = row.get("Kund", "")
                if kund_value:
                    same_kund = df[df["Kund"] == kund_value]
                    related_numbers = (
                        same_kund[number_col]
                        .dropna()
                        .astype(str)
                        .str.strip()
                        .unique()
                        .tolist()
                    )
                    related_numbers = [n for n in related_numbers if n != number]

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "q": q,
            "results": results,
            "display_sheets": DISPLAY_SHEETS,
            "dashboard": dashboard,
            "allocation": allocation,
            "holdings_total": holdings_total,
            "alternativa_total": alternativa_total,
            "alternativa_share": alternativa_share,
            "core_sverige_share": core_sverige_share,
            "core_varlden_share": core_varlden_share,
            "edge_share": edge_share,
            "fixed_income_share": fixed_income_share,
            "ovrigt_share": ovrigt_share,
            "fixed_income_model": fixed_income_model,
            "valuta_total": valuta_total,
            "total_share": total_share,
            "valuta_share": valuta_share,
            "modul_totals": modul_totals,
            "tillgang_totals": tillgang_totals,
            "post_by_modul": post_by_modul,
            "portfolio_modul_counts": portfolio_modul_counts,
            "model_modul_counts": model_modul_counts,
            "missing_by_modul": missing_by_modul,
            "has_holdings": has_holdings,
            "related_numbers": related_numbers,
            "number_suggestions": number_suggestions,
            "format_cell": format_cell,
            "format_percent": format_percent,
        },
    )


@app.get("/taggar", response_class=HTMLResponse)
def taggar(request: Request):
    df = _load_taggar_table()
    cols = [c for c in TAGGAR_COLUMNS if c in df.columns]
    if not df.empty and "Modul" in df.columns:
        df = df.sort_values(by="Modul", kind="stable")
    rows = _safe_rows(df[["row_id"] + cols]) if not df.empty else []
    detaljerat = _load_sheet("Detaljerat")
    def _normalize_name(value: str) -> str:
        return (
            str(value)
            .replace("\u00a0", " ")
            .strip()
            .casefold()
        )

    taggar_series = df["Short Name"].dropna().astype(str).str.strip() if not df.empty else []
    detaljerat_series = detaljerat["Short Name"].dropna().astype(str).str.strip()
    currency_series = detaljerat["Currency"].dropna().astype(str).str.strip() if "Currency" in detaljerat.columns else []

    taggar_map = {_normalize_name(s): s for s in taggar_series}
    detaljerat_map = {_normalize_name(s): s for s in detaljerat_series}
    currency_map = {_normalize_name(s): s for s in currency_series}
    taggar_set = set(taggar_map.keys())
    detaljerat_set = set(detaljerat_map.keys()) | set(currency_map.keys())
    instrument_type_map = {}
    if "Instrument Type" in detaljerat.columns:
        for short_name, inst_type in zip(detaljerat_series, detaljerat["Instrument Type"]):
            key = _normalize_name(short_name)
            if key and key not in instrument_type_map:
                instrument_type_map[key] = str(inst_type).strip()

    only_in_detaljerat = [
        {
            "name": detaljerat_map.get(k, k),
            "instrument_type": instrument_type_map.get(k, ""),
        }
        for k in sorted(detaljerat_set - taggar_set)
    ]
    only_in_taggar = sorted(taggar_map[k] for k in (taggar_set - detaljerat_set))
    return templates.TemplateResponse(
        "taggar.html",
        {
            "request": request,
            "columns": cols,
            "rows": rows,
            "only_in_detaljerat": only_in_detaljerat,
            "only_in_taggar": only_in_taggar,
        },
    )


@app.get("/fixed-income", response_class=HTMLResponse)
def fixed_income(request: Request, sort_by: str = "att_kopa"):
    mandat = _load_sheet("Mandat")
    detaljerat = _load_sheet("Detaljerat")
    taggar_df = _load_sheet("Taggar")
    taggar_map = {}
    if "Short Name" in taggar_df.columns:
        for _, row in taggar_df.iterrows():
            key = _normalize_key(row.get("Short Name", ""))
            if not key:
                continue
            taggar_map[key] = row.to_dict()
    detaljerat = _load_sheet("Detaljerat")
    taggar_df = _load_sheet("Taggar")

    taggar_map = {}
    currency_map = {}
    if "Short Name" in taggar_df.columns:
        for _, row in taggar_df.iterrows():
            key = _normalize_key(row.get("Short Name", ""))
            if not key:
                continue
            taggar_map[key] = row.to_dict()
            kurs = pd.to_numeric(row.get("Kurs", None), errors="coerce")
            if pd.notna(kurs):
                currency_map[key] = float(kurs)

    rows = []
    for _, row in mandat.iterrows():
        number = row.get("Number", row.get("Nummer", ""))
        if number == "" or pd.isna(number):
            continue

        details = detaljerat[detaljerat["Number"].astype(str).str.strip() == str(number).strip()]
        fi_share = 0.0
        holdings_total = 0.0
        valuta_total = 0.0
        portfolio_other_sum = 0.0
        if not details.empty:
            modul = details["Short Name"].apply(
                lambda s: taggar_map.get(_normalize_key(s), {}).get("Modul", "")
            ).astype(str).str.strip().str.lower()
            modul = modul.replace({"": "övrigt", "nan": "övrigt"})

            counts = pd.to_numeric(details["Available Count"], errors="coerce")
            prices = pd.to_numeric(details["Price"], errors="coerce")
            base_value = counts * prices
            base_value = base_value.where(modul != "fixed income", base_value / 100)

            if "Currency" in details.columns:
                rates = details["Currency"].apply(
                    lambda c: currency_map.get(_normalize_key(c), 1.0)
                )
                base_value = base_value * pd.to_numeric(rates, errors="coerce").fillna(1.0)

            holdings_total = base_value.sum(skipna=True)
            alt_total = base_value.where(modul == "alternativa").sum(skipna=True)
            cs_total = base_value.where(modul == "core sverige").sum(skipna=True)
            cv_total = base_value.where(modul == "core världen").sum(skipna=True)
            edge_total = base_value.where(modul == "edge").sum(skipna=True)
            ovrigt_total = base_value.where(modul == "övrigt").sum(skipna=True)
            fi_total = base_value.where(modul == "fixed income").sum(skipna=True)

            fi_share = (fi_total / holdings_total) if holdings_total else 0.0
            if holdings_total:
                portfolio_other_sum = (alt_total + cs_total + cv_total + edge_total + ovrigt_total) / holdings_total
            tillgang = details["Short Name"].apply(
                lambda s: taggar_map.get(_normalize_key(s), {}).get("Tillgångsslag", "")
            ).astype(str).str.strip()
            if "Instrument Type" in details.columns:
                fallback = (
                    details["Instrument Type"]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .map({"share": "Aktier", "bond": "Ränta", "fund": "Aktier", "etf": "Aktier"})
                    .fillna("")
                )
                tillgang = tillgang.where(tillgang != "", fallback)
            valuta_total = base_value.where(tillgang.str.lower() == "valuta").sum(skipna=True)

        model_sum = sum(
            v for v in [
                _to_float(row.get("Alt", "")),
                _to_float(row.get("CS", "")),
                _to_float(row.get("CV", "")),
                _to_float(row.get("Ed", "")),
            ]
            if v is not None
        )
        fixed_income_model = max(0, 1 - max(model_sum, portfolio_other_sum))
        kassa_fi = (fixed_income_model - fi_share) * holdings_total if holdings_total else 0.0
        fi_count = 0
        if not details.empty:
            fi_count = (
                details["Short Name"]
                .apply(lambda s: taggar_map.get(_normalize_key(s), {}).get("Modul", ""))
                .astype(str)
                .str.strip()
                .str.lower()
                .eq("fixed income")
                .sum()
            )
        position_fi = (fi_total / fi_count) if fi_count else 0.0
        poster_fi = (kassa_fi / position_fi) if position_fi else 0.0
        if str(row.get("Mandat", "")).strip().lower() == "matardepå":
            poster_fi = ""

        if position_fi:
            fi_position_value = max(50000, round(position_fi / 50000) * 50000)
        else:
            fi_position_value = 0.0
        if (
            fixed_income_model > fi_share
            and kassa_fi > 50000
            and fixed_income_model >= 0.09
            and str(row.get("Mandat", "")).strip().lower() != "aktier"
            and holdings_total >= 200000
        ):
            rows.append(
                {
                    "Number": number,
                    "Kund": row.get("Kund", ""),
                    "Mandat": row.get("Mandat", ""),
                    "Kassa": valuta_total,
                    "Kassa FI": kassa_fi,
                    "Att köpa": poster_fi,
                    "FI-notering": "" if pd.isna(row.get("FI-notering", "")) else row.get("FI-notering", ""),
                    "Position FI": fi_position_value,
                }
            )

    columns = ["Number", "Kund", "Mandat", "Kassa", "Kassa FI", "Att köpa", "Position FI", "FI-notering"]
    sort_key = sort_by.lower()
    if sort_key not in {"att_kopa", "kassa_fi", "poster_fi", "number"}:
        sort_key = "att_kopa"
    field_map = {"att_kopa": "Att köpa", "poster_fi": "Att köpa", "kassa_fi": "Kassa FI", "number": "Number"}
    sort_field = field_map[sort_key]
    kassa_fi_matardepo = sum(
        _to_float(r.get("Kassa FI", 0)) or 0
        for r in rows
        if str(r.get("Mandat", "")).strip().lower() == "matardepå"
    )
    matardepo_rows = [
        {
            "Number": r.get("Number", ""),
            "Kund": r.get("Kund", ""),
            "Kassa FI": r.get("Kassa FI", 0),
        }
        for r in rows
        if str(r.get("Mandat", "")).strip().lower() == "matardepå"
    ]
    matardepo_by_kund = {}
    for r in rows:
        if str(r.get("Mandat", "")).strip().lower() == "matardepå":
            kund_key = str(r.get("Kund", "")).strip()
            matardepo_by_kund[kund_key] = r.get("Kassa FI", 0)

    for r in rows:
        if str(r.get("Mandat", "")).strip().lower() != "matardepå":
            kund_key = str(r.get("Kund", "")).strip()
            if kund_key in matardepo_by_kund:
                r["Kassa FI Matardepå"] = matardepo_by_kund[kund_key]
                position_val = _to_float(r.get("Position FI", 0)) or 0
                if position_val:
                    combined_kassa = (_to_float(r.get("Kassa FI", 0)) or 0) + (_to_float(r.get("Kassa FI Matardepå", 0)) or 0)
                    r["Att köpa"] = combined_kassa / position_val
            else:
                r["Kassa FI Matardepå"] = None

    if sort_key == "number":
        rows = sorted(
            rows,
            key=lambda r: (_to_float(r.get("Number", 0)) or 0),
            reverse=False,
        )
    else:
        rows = sorted(rows, key=lambda r: _to_float(r.get(sort_field, 0)) or 0, reverse=True)

    kassa_fi_sum = sum(
        _to_float(r.get("Kassa FI", 0)) or 0
        for r in rows
        if (_to_float(r.get("Att köpa", 0)) or 0) > 1
    )

    return templates.TemplateResponse(
        "fixed_income.html",
        {
            "request": request,
            "columns": columns,
            "rows": rows,
            "format_cell": format_cell,
            "sort_by": sort_key,
            "kassa_fi_sum": kassa_fi_sum,
            "kassa_fi_matardepo": kassa_fi_matardepo,
            "matardepo_rows": matardepo_rows,
        },
    )


@app.get("/ombalansering", response_class=HTMLResponse)
def ombalansering(request: Request, modul: str = "", q: str = ""):
    mandat = _load_sheet("Mandat")
    detaljerat = _load_sheet("Detaljerat")
    taggar_df = _load_sheet("Taggar")
    flags_df = _load_mandat_flags()
    dyn_df = _load_mandat_dyn()
    taggar_map = {}
    currency_map = {}
    if "Short Name" in taggar_df.columns:
        for _, row in taggar_df.iterrows():
            key = _normalize_key(row.get("Short Name", ""))
            if not key:
                continue
            taggar_map[key] = row.to_dict()
            kurs = pd.to_numeric(row.get("Kurs", None), errors="coerce")
            if pd.notna(kurs):
                currency_map[key] = float(kurs)
    modul_key = modul.strip().lower()
    modul_map = {
        "core-sverige": ("CS", "Core Sverige"),
        "core-varlden": ("CV", "Core Världen"),
        "edge": ("Ed", "Edge"),
        "alternativa": ("Alt", "Alternativa"),
    }
    selected = modul_map.get(modul_key)
    rows = []
    strategi_items = []
    try:
        strategi_df = _load_strategi()
        if not strategi_df.empty:
            strategi_row = strategi_df.iloc[0].where(pd.notna(strategi_df.iloc[0]), "")
            for col in strategi_df.columns:
                strategi_items.append({"label": str(col).strip(), "value": strategi_row.get(col, "")})
    except Exception:
        strategi_items = []
    if selected:
        col, label = selected
        if not flags_df.empty and "number" in flags_df.columns:
            number_col = "Number" if "Number" in mandat.columns else "Nummer"
            flags_merge = flags_df.rename(
                columns={
                    "number": number_col,
                    "dynamisk": "dynamisk",
                    "coresv": "coresv",
                    "coreva": "corevä",
                    "edge": "edge",
                    "alts": "alts",
                }
            )
            flags_merge[number_col] = flags_merge[number_col].astype(str).str.strip()
            mandat[number_col] = mandat[number_col].astype(str).str.strip()
            mandat = mandat.merge(flags_merge, on=number_col, how="left", suffixes=("", "_flag"))
            for flag_col, base_col in [
                ("dynamisk_flag", "dynamisk"),
                ("coresv_flag", "coresv"),
                ("corevä_flag", "corevä"),
                ("edge_flag", "edge"),
                ("alts_flag", "alts"),
            ]:
                if flag_col in mandat.columns:
                    if base_col in mandat.columns:
                        mandat[base_col] = mandat[flag_col].fillna(mandat[base_col])
                    else:
                        mandat[base_col] = mandat[flag_col]
                    mandat.drop(columns=[flag_col], inplace=True)
        if not dyn_df.empty and "number" in dyn_df.columns:
            number_col = "Number" if "Number" in mandat.columns else "Nummer"
            dyn_merge = dyn_df.rename(columns={"number": number_col})
            dyn_merge[number_col] = dyn_merge[number_col].astype(str).str.strip()
            mandat[number_col] = mandat[number_col].astype(str).str.strip()
            mandat = mandat.merge(dyn_merge, on=number_col, how="left", suffixes=("", "_dyn"))
            for dcol in ["dynCS", "dynCV", "dynEd", "dynAlt"]:
                dcol_dyn = f"{dcol}_dyn"
                if dcol_dyn in mandat.columns:
                    if dcol in mandat.columns:
                        mandat[dcol] = mandat[dcol_dyn].fillna(mandat[dcol])
                    else:
                        mandat[dcol] = mandat[dcol_dyn].fillna(0)
                    mandat.drop(columns=[dcol_dyn], inplace=True)
        for col_name in ["dynamisk", "dynCS", "dynCV", "dynEd", "dynAlt"]:
            if col_name not in mandat.columns:
                mandat[col_name] = 0

        dyn_col_map = {"CS": "dynCS", "CV": "dynCV", "Ed": "dynEd", "Alt": "dynAlt"}
        eff_col = dyn_col_map.get(col, col)
        mandat["__model_value"] = pd.to_numeric(mandat[col], errors="coerce").fillna(0)
        dyn_vals = pd.to_numeric(mandat[eff_col], errors="coerce").fillna(0)
        mandat["__model_value"] = mandat["__model_value"].where(mandat["dynamisk"] != 1, dyn_vals)
        subset = mandat[mandat["__model_value"].notna() & (mandat["__model_value"] > 0)]
        # Build row per holding for selected modul
        if "Short Name" in detaljerat.columns:
            modul_series = detaljerat["Short Name"].apply(
                lambda s: taggar_map.get(_normalize_key(s), {}).get("Modul", "")
            ).astype(str).str.strip().str.lower()
            modul_series = modul_series.replace({"": "övrigt", "nan": "övrigt"})
            modul_match = modul_series == label.lower()
            detail_subset = detaljerat[modul_match].copy()
            name_col = "Instrument Name" if "Instrument Name" in detail_subset.columns else "Short Name"
            detail_subset["Number"] = detail_subset["Number"].apply(_normalize_number_value)
            # compute holding value in SEK (selected module rows)
            counts = pd.to_numeric(detail_subset.get("Available Count", 0), errors="coerce")
            prices = pd.to_numeric(detail_subset.get("Price", 0), errors="coerce")
            base_value = counts * prices
            base_value = base_value.where(modul_series[modul_match] != "fixed income", base_value / 100)
            if "Currency" in detail_subset.columns:
                rates = detail_subset["Currency"].apply(
                    lambda c: currency_map.get(_normalize_key(c), 1.0)
                )
                base_value = base_value * pd.to_numeric(rates, errors="coerce").fillna(1.0)
            detail_subset["Värde (sek)"] = base_value

            mandate_map = (
                subset.set_index(subset["Number"].apply(_normalize_number_value))[
                    ["Kund", "Mandat", "__model_value"]
                ]
                .to_dict(orient="index")
            )
            model_weight_by_holding = _model_weights_for_modul(label, taggar_df)
            # holdings total per portfolio (all holdings)
            totals_by_number = {}
            if "Short Name" in detaljerat.columns:
                full_counts = pd.to_numeric(detaljerat.get("Available Count", 0), errors="coerce")
                full_prices = pd.to_numeric(detaljerat.get("Price", 0), errors="coerce")
                full_base = full_counts * full_prices
                full_modul = detaljerat["Short Name"].apply(
                    lambda s: taggar_map.get(_normalize_key(s), {}).get("Modul", "")
                ).astype(str).str.strip().str.lower().replace({"": "övrigt", "nan": "övrigt"})
                full_base = full_base.where(full_modul != "fixed income", full_base / 100)
                if "Currency" in detaljerat.columns:
                    full_rates = detaljerat["Currency"].apply(
                        lambda c: currency_map.get(_normalize_key(c), 1.0)
                    )
                    full_base = full_base * pd.to_numeric(full_rates, errors="coerce").fillna(1.0)
                total_numbers = detaljerat["Number"].apply(_normalize_number_value)
                totals_by_number = (
                    pd.DataFrame({"Number": total_numbers, "Value": full_base})
                    .groupby("Number")["Value"]
                    .sum()
                    .to_dict()
                )
            # build list of holdings for this modul from Taggar (one row per holding per number)
            holding_display_map = {}
            holding_norm_list = []
            if "Short Name" in taggar_df.columns:
                taggar_modul = (
                    taggar_df[taggar_df["Modul"].astype(str).str.strip().str.lower() == label.lower()]
                    if "Modul" in taggar_df.columns
                    else taggar_df
                )
                for short in taggar_modul["Short Name"].dropna().astype(str).str.strip():
                    norm = _normalize_holding_name(short)
                    if norm not in holding_display_map:
                        holding_display_map[norm] = short
                        holding_norm_list.append(norm)
            # map (number, holding) -> value
            value_by_key = {}
            for _, drow in detail_subset.iterrows():
                number = _normalize_number_value(drow.get("Number", ""))
                if "Short Name" in detail_subset.columns:
                    holding = _normalize_holding_name(drow.get("Short Name", ""))
                else:
                    holding = _normalize_holding_name(drow.get(name_col, ""))
                value_by_key[(number, holding)] = drow.get("Värde (sek)", 0) or 0

            for number, info in mandate_map.items():
                model_value = _to_float(info.get("__model_value", 0)) or 0
                holdings_total = totals_by_number.get(number, 0) or 0
                for holding_norm in holding_norm_list:
                    holding_value = value_by_key.get((number, holding_norm), 0)
                    weight = model_weight_by_holding.get(holding_norm, 0)
                    position_value = holdings_total * model_value * weight if holdings_total and model_value > 0 else 0
                    rows.append(
                        {
                            "Number": number,
                            "Kund": info.get("Kund", ""),
                            "Mandat": info.get("Mandat", ""),
                            "Innehav": holding_display_map.get(holding_norm, holding_norm),
                            "Värde (sek)": holding_value,
                            "Modell": position_value,
                            "vs modell": holding_value - position_value,
                        }
                    )
    rows = [
        r
        for r in rows
        if abs(_to_float(r.get("vs modell", 0)) or 0) >= 13000
    ]
    if q:
        q_norm = q.strip()
        rows = [r for r in rows if str(r.get("Number", "")).startswith(q_norm)]
    columns = ["Number", "Kund", "Mandat", "Innehav", "Värde (sek)", "Modell", "vs modell"]
    return templates.TemplateResponse(
        "ombalansering.html",
        {
            "request": request,
            "columns": columns,
            "rows": rows,
            "q": q,
            "selected_modul": modul_key,
            "selected_label": selected[1] if selected else "",
            "format_cell": format_cell,
            "strategi_items": strategi_items,
        },
    )


@app.get("/ombalansering/export")
def ombalansering_export(modul: str = ""):
    mandat = _load_sheet("Mandat")
    detaljerat = _load_sheet("Detaljerat")
    taggar_df = _load_sheet("Taggar")
    flags_df = _load_mandat_flags()
    dyn_df = _load_mandat_dyn()
    taggar_map = {}
    currency_map = {}
    if "Short Name" in taggar_df.columns:
        for _, row in taggar_df.iterrows():
            key = _normalize_key(row.get("Short Name", ""))
            if not key:
                continue
            taggar_map[key] = row.to_dict()
            kurs = pd.to_numeric(row.get("Kurs", None), errors="coerce")
            if pd.notna(kurs):
                currency_map[key] = float(kurs)
    modul_key = modul.strip().lower()
    modul_map = {
        "core-sverige": ("CS", "Core Sverige"),
        "core-varlden": ("CV", "Core Världen"),
        "edge": ("Ed", "Edge"),
        "alternativa": ("Alt", "Alternativa"),
    }
    selected = modul_map.get(modul_key)
    rows = []
    if selected:
        col, label = selected
        if not flags_df.empty and "number" in flags_df.columns:
            number_col = "Number" if "Number" in mandat.columns else "Nummer"
            flags_merge = flags_df.rename(
                columns={
                    "number": number_col,
                    "dynamisk": "dynamisk",
                    "coresv": "coresv",
                    "coreva": "corevä",
                    "edge": "edge",
                    "alts": "alts",
                }
            )
            flags_merge[number_col] = flags_merge[number_col].astype(str).str.strip()
            mandat[number_col] = mandat[number_col].astype(str).str.strip()
            mandat = mandat.merge(flags_merge, on=number_col, how="left", suffixes=("", "_flag"))
            for flag_col, base_col in [
                ("dynamisk_flag", "dynamisk"),
                ("coresv_flag", "coresv"),
                ("corevä_flag", "corevä"),
                ("edge_flag", "edge"),
                ("alts_flag", "alts"),
            ]:
                if flag_col in mandat.columns:
                    if base_col in mandat.columns:
                        mandat[base_col] = mandat[flag_col].fillna(mandat[base_col])
                    else:
                        mandat[base_col] = mandat[flag_col]
                    mandat.drop(columns=[flag_col], inplace=True)
        if not dyn_df.empty and "number" in dyn_df.columns:
            number_col = "Number" if "Number" in mandat.columns else "Nummer"
            dyn_merge = dyn_df.rename(columns={"number": number_col})
            dyn_merge[number_col] = dyn_merge[number_col].astype(str).str.strip()
            mandat[number_col] = mandat[number_col].astype(str).str.strip()
            mandat = mandat.merge(dyn_merge, on=number_col, how="left", suffixes=("", "_dyn"))
            for dcol in ["dynCS", "dynCV", "dynEd", "dynAlt"]:
                dcol_dyn = f"{dcol}_dyn"
                if dcol_dyn in mandat.columns:
                    if dcol in mandat.columns:
                        mandat[dcol] = mandat[dcol_dyn].fillna(mandat[dcol])
                    else:
                        mandat[dcol] = mandat[dcol_dyn].fillna(0)
                    mandat.drop(columns=[dcol_dyn], inplace=True)
        for col_name in ["dynamisk", "dynCS", "dynCV", "dynEd", "dynAlt"]:
            if col_name not in mandat.columns:
                mandat[col_name] = 0

        dyn_col_map = {"CS": "dynCS", "CV": "dynCV", "Ed": "dynEd", "Alt": "dynAlt"}
        eff_col = dyn_col_map.get(col, col)
        mandat["__model_value"] = pd.to_numeric(mandat[col], errors="coerce").fillna(0)
        dyn_vals = pd.to_numeric(mandat[eff_col], errors="coerce").fillna(0)
        mandat["__model_value"] = mandat["__model_value"].where(mandat["dynamisk"] != 1, dyn_vals)
        subset = mandat[mandat["__model_value"].notna() & (mandat["__model_value"] > 0)]
        if "Short Name" in detaljerat.columns:
            modul_series = detaljerat["Short Name"].apply(
                lambda s: taggar_map.get(_normalize_key(s), {}).get("Modul", "")
            ).astype(str).str.strip().str.lower()
            modul_series = modul_series.replace({"": "övrigt", "nan": "övrigt"})
            modul_match = modul_series == label.lower()
            detail_subset = detaljerat[modul_match].copy()
            name_col = "Instrument Name" if "Instrument Name" in detail_subset.columns else "Short Name"
            detail_subset["Number"] = detail_subset["Number"].apply(_normalize_number_value)
            counts = pd.to_numeric(detail_subset.get("Available Count", 0), errors="coerce")
            prices = pd.to_numeric(detail_subset.get("Price", 0), errors="coerce")
            base_value = counts * prices
            base_value = base_value.where(modul_series[modul_match] != "fixed income", base_value / 100)
            if "Currency" in detail_subset.columns:
                rates = detail_subset["Currency"].apply(
                    lambda c: currency_map.get(_normalize_key(c), 1.0)
                )
                base_value = base_value * pd.to_numeric(rates, errors="coerce").fillna(1.0)
            detail_subset["Värde (sek)"] = base_value

            mandate_map = (
                subset.set_index(subset["Number"].apply(_normalize_number_value))[
                    ["Kund", "Mandat", "__model_value"]
                ]
                .to_dict(orient="index")
            )
            model_weight_by_holding = _model_weights_for_modul(label, taggar_df)
            totals_by_number = {}
            if "Short Name" in detaljerat.columns:
                full_counts = pd.to_numeric(detaljerat.get("Available Count", 0), errors="coerce")
                full_prices = pd.to_numeric(detaljerat.get("Price", 0), errors="coerce")
                full_base = full_counts * full_prices
                full_modul = detaljerat["Short Name"].apply(
                    lambda s: taggar_map.get(_normalize_key(s), {}).get("Modul", "")
                ).astype(str).str.strip().str.lower().replace({"": "övrigt", "nan": "övrigt"})
                full_base = full_base.where(full_modul != "fixed income", full_base / 100)
                if "Currency" in detaljerat.columns:
                    full_rates = detaljerat["Currency"].apply(
                        lambda c: currency_map.get(_normalize_key(c), 1.0)
                    )
                    full_base = full_base * pd.to_numeric(full_rates, errors="coerce").fillna(1.0)
                total_numbers = detaljerat["Number"].apply(_normalize_number_value)
                totals_by_number = (
                    pd.DataFrame({"Number": total_numbers, "Value": full_base})
                    .groupby("Number")["Value"]
                    .sum()
                    .to_dict()
                )
            holding_display_map = {}
            holding_norm_list = []
            if "Short Name" in taggar_df.columns:
                taggar_modul = (
                    taggar_df[taggar_df["Modul"].astype(str).str.strip().str.lower() == label.lower()]
                    if "Modul" in taggar_df.columns
                    else taggar_df
                )
                for short in taggar_modul["Short Name"].dropna().astype(str).str.strip():
                    norm = _normalize_holding_name(short)
                    if norm not in holding_display_map:
                        holding_display_map[norm] = short
                        holding_norm_list.append(norm)
            value_by_key = {}
            for _, drow in detail_subset.iterrows():
                number = _normalize_number_value(drow.get("Number", ""))
                if "Short Name" in detail_subset.columns:
                    holding = _normalize_holding_name(drow.get("Short Name", ""))
                else:
                    holding = _normalize_holding_name(drow.get(name_col, ""))
                value_by_key[(number, holding)] = drow.get("Värde (sek)", 0) or 0

            for number, info in mandate_map.items():
                model_value = _to_float(info.get("__model_value", 0)) or 0
                holdings_total = totals_by_number.get(number, 0) or 0
                for holding_norm in holding_norm_list:
                    holding_value = value_by_key.get((number, holding_norm), 0)
                    weight = model_weight_by_holding.get(holding_norm, 0)
                    position_value = (
                        holdings_total * model_value * weight
                        if holdings_total and model_value > 0
                        else 0
                    )
                    rows.append(
                        {
                            "Number": number,
                            "Kund": info.get("Kund", ""),
                            "Mandat": info.get("Mandat", ""),
                            "Innehav": holding_display_map.get(holding_norm, holding_norm),
                            "Värde (sek)": holding_value,
                            "Modell": position_value,
                            "vs modell": holding_value - position_value,
                        }
                    )

    rows = [
        r
        for r in rows
        if abs(_to_float(r.get("vs modell", 0)) or 0) >= 13000
    ]
    columns = ["Number", "Kund", "Mandat", "Innehav", "Värde (sek)", "Modell", "vs modell"]
    df = pd.DataFrame(rows, columns=columns)
    output = BytesIO()
    df.to_excel(output, index=False, sheet_name="Ombalansering")
    output.seek(0)
    headers = {"Content-Disposition": "attachment; filename=ombalansering.xlsx"}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@app.get("/modulforandring", response_class=HTMLResponse)
def modulforandring(request: Request, modul: str = "", q: str = ""):
    mandat = _load_sheet("Mandat")
    detaljerat = _load_sheet("Detaljerat")
    taggar_df = _load_sheet("Taggar")
    flags_df = _load_mandat_flags()
    dyn_df = _load_mandat_dyn()
    taggar_map = {}
    currency_map = {}
    if "Short Name" in taggar_df.columns:
        for _, row in taggar_df.iterrows():
            key = _normalize_key(row.get("Short Name", ""))
            if not key:
                continue
            taggar_map[key] = row.to_dict()
            kurs = pd.to_numeric(row.get("Kurs", None), errors="coerce")
            if pd.notna(kurs):
                currency_map[key] = float(kurs)

    modul_key = modul.strip().lower()
    modul_map = {
        "core-sverige": ("CS", "Core Sverige"),
        "core-varlden": ("CV", "Core Världen"),
        "edge": ("Ed", "Edge"),
        "alternativa": ("Alt", "Alternativa"),
    }
    selected = modul_map.get(modul_key)
    rows = []
    if selected:
        col, label = selected
        if not flags_df.empty and "number" in flags_df.columns:
            number_col = "Number" if "Number" in mandat.columns else "Nummer"
            flags_merge = flags_df.rename(
                columns={
                    "number": number_col,
                    "dynamisk": "dynamisk",
                    "coresv": "coresv",
                    "coreva": "corevä",
                    "edge": "edge",
                    "alts": "alts",
                }
            )
            flags_merge[number_col] = flags_merge[number_col].astype(str).str.strip()
            mandat[number_col] = mandat[number_col].astype(str).str.strip()
            mandat = mandat.merge(flags_merge, on=number_col, how="left", suffixes=("", "_flag"))
            for flag_col, base_col in [
                ("dynamisk_flag", "dynamisk"),
                ("coresv_flag", "coresv"),
                ("corevä_flag", "corevä"),
                ("edge_flag", "edge"),
                ("alts_flag", "alts"),
            ]:
                if flag_col in mandat.columns:
                    if base_col in mandat.columns:
                        mandat[base_col] = mandat[flag_col].fillna(mandat[base_col])
                    else:
                        mandat[base_col] = mandat[flag_col]
                    mandat.drop(columns=[flag_col], inplace=True)
        if not dyn_df.empty and "number" in dyn_df.columns:
            number_col = "Number" if "Number" in mandat.columns else "Nummer"
            dyn_merge = dyn_df.rename(columns={"number": number_col})
            dyn_merge[number_col] = dyn_merge[number_col].astype(str).str.strip()
            mandat[number_col] = mandat[number_col].astype(str).str.strip()
            mandat = mandat.merge(dyn_merge, on=number_col, how="left", suffixes=("", "_dyn"))
            for dcol in ["dynCS", "dynCV", "dynEd", "dynAlt"]:
                dcol_dyn = f"{dcol}_dyn"
                if dcol_dyn in mandat.columns:
                    if dcol in mandat.columns:
                        mandat[dcol] = mandat[dcol_dyn].fillna(mandat[dcol])
                    else:
                        mandat[dcol] = mandat[dcol_dyn].fillna(0)
                    mandat.drop(columns=[dcol_dyn], inplace=True)
        for col_name in ["dynamisk", "dynCS", "dynCV", "dynEd", "dynAlt"]:
            if col_name not in mandat.columns:
                mandat[col_name] = 0

        dyn_col_map = {"CS": "dynCS", "CV": "dynCV", "Ed": "dynEd", "Alt": "dynAlt"}
        eff_col = dyn_col_map.get(col, col)
        mandat["__model_value"] = pd.to_numeric(mandat[col], errors="coerce").fillna(0)
        dyn_vals = pd.to_numeric(mandat[eff_col], errors="coerce").fillna(0)
        mandat["__model_value"] = mandat["__model_value"].where(mandat["dynamisk"] != 1, dyn_vals)
        subset = mandat[mandat["__model_value"].notna() & (mandat["__model_value"] > 0)]

        # model count for module (from Taggar)
        modul_counts = (
            taggar_df["Modul"]
            .dropna()
            .astype(str)
            .str.strip()
            .str.lower()
            .value_counts()
            .to_dict()
        )
        model_count = modul_counts.get(label.lower(), 0)

        # totals per portfolio (holdings total + kassa)
        totals_by_number = {}
        kassa_by_number = {}
        if "Short Name" in detaljerat.columns:
            full_counts = pd.to_numeric(detaljerat.get("Available Count", 0), errors="coerce")
            full_prices = pd.to_numeric(detaljerat.get("Price", 0), errors="coerce")
            full_base = full_counts * full_prices
            full_modul = detaljerat["Short Name"].apply(
                lambda s: taggar_map.get(_normalize_key(s), {}).get("Modul", "")
            ).astype(str).str.strip().str.lower().replace({"": "övrigt", "nan": "övrigt"})
            full_base = full_base.where(full_modul != "fixed income", full_base / 100)
            if "Currency" in detaljerat.columns:
                full_rates = detaljerat["Currency"].apply(
                    lambda c: currency_map.get(_normalize_key(c), 1.0)
                )
                full_base = full_base * pd.to_numeric(full_rates, errors="coerce").fillna(1.0)

            total_numbers = detaljerat["Number"].astype(str).str.strip()
            totals_by_number = (
                pd.DataFrame({"Number": total_numbers, "Value": full_base})
                .groupby("Number")["Value"]
                .sum()
                .to_dict()
            )

            tillgang = detaljerat["Short Name"].apply(
                lambda s: taggar_map.get(_normalize_key(s), {}).get("Tillgångsslag", "")
            ).astype(str).str.strip()
            if "Instrument Type" in detaljerat.columns:
                fallback = (
                    detaljerat["Instrument Type"]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .map({"share": "Aktier", "bond": "Ränta", "fund": "Aktier", "etf": "Aktier"})
                    .fillna("")
                )
                tillgang = tillgang.where(tillgang != "", fallback)
            kassa_by_number = (
                pd.DataFrame({"Number": total_numbers, "Value": full_base, "Tillgang": tillgang})
                .loc[lambda d: d["Tillgang"].str.lower() == "valuta"]
                .groupby("Number")["Value"]
                .sum()
                .to_dict()
            )

        for _, row in subset.iterrows():
            number = str(row.get("Number", row.get("Nummer", ""))).strip()
            model_value = _to_float(row.get("__model_value", 0)) or 0
            holdings_total = totals_by_number.get(number, 0) or 0
            position_value = (
                holdings_total * model_value / model_count
                if holdings_total and model_value > 0 and model_count
                else 0
            )
            rows.append(
                {
                    "Number": number,
                    "Kund": row.get("Kund", ""),
                    "Modul": label,
                    "Kassa": kassa_by_number.get(number, 0),
                    "Position": position_value,
                }
            )

    if q:
        q_norm = q.strip()
        rows = [r for r in rows if str(r.get("Number", "")).startswith(q_norm)]

    columns = ["Number", "Kund", "Modul", "Kassa", "Position"]
    position_sum = sum(_to_float(r.get("Position", 0)) or 0 for r in rows)
    return templates.TemplateResponse(
        "modulforandring.html",
        {
            "request": request,
            "selected_modul": modul_key,
            "selected_label": selected[1] if selected else "",
            "columns": columns,
            "rows": rows,
            "format_cell": format_cell,
            "position_sum": position_sum,
            "q": q,
        },
    )


@app.get("/mandat", response_class=HTMLResponse)
def mandat_page(request: Request, q: str = "", sort_by: str = "", compliance: str = ""):
    number = q.strip()
    df = _load_sheet("Mandat")
    number_col = "Number" if "Number" in df.columns else "Nummer"
    for col in FLAG_COLUMNS:
        if col not in df.columns:
            df[col] = 0
    for col in MANDAT_BOOL_COLUMNS:
        if col not in df.columns:
            df[col] = 0
    bool_cols = ["RG7>25", "20%", "Akt>75", "Akt>25", "Alt>50", "Rä != 0", "Alt!= 0", "Placeringsriktlinjer"]
    falsy_values = {"", "0", "false", "False", "FALSE", "nan", "NaN", None}
    for col in bool_cols:
        if col in df.columns:
            def _to_bool_int(v):
                if pd.isna(v):
                    return 0
                text = str(v).strip()
                if text in falsy_values:
                    return 0
                try:
                    num = float(text)
                    return 0 if num == 0 else 1
                except (TypeError, ValueError):
                    return 1
            df[col] = df[col].apply(_to_bool_int)
    for col in ["dynCS", "dynCV", "dynEd", "dynAlt"]:
        if col not in df.columns:
            df[col] = 0
    strategi_vals = {}
    try:
        strategi_df = _load_strategi()
        if not strategi_df.empty:
            strategi_row = strategi_df.iloc[0].where(pd.notna(strategi_df.iloc[0]), "")
            strategi_vals = {
                "coresv": _to_float(strategi_row.get("Core Sverige", 0)) or 0,
                "corevä": _to_float(strategi_row.get("Core Världen", 0)) or 0,
                "edge": _to_float(strategi_row.get("Edge", 0)) or 0,
                "alts": _to_float(strategi_row.get("Alternativa", 0)) or 0,
            }
    except Exception:
        strategi_vals = {}
    flags_df = _load_mandat_flags()
    if not flags_df.empty and number_col in df.columns and "number" in flags_df.columns:
        flags_df = flags_df.rename(
            columns={
                "number": number_col,
                "dynamisk": "dynamisk",
                "coresv": "coresv",
                "coreva": "corevä",
                "edge": "edge",
                "alts": "alts",
            }
        )
        flags_df[number_col] = flags_df[number_col].astype(str).str.strip()
        df[number_col] = df[number_col].astype(str).str.strip()
        df = df.merge(flags_df, on=number_col, how="left", suffixes=("", "_flag"))
        for col in FLAG_COLUMNS:
            flag_col = f"{col}_flag"
            if flag_col in df.columns:
                df[col] = df[flag_col].fillna(df[col])
                df.drop(columns=[flag_col], inplace=True)
        for col in FLAG_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    dyn_df = _load_mandat_dyn()
    if not dyn_df.empty and number_col in df.columns and "number" in dyn_df.columns:
        dyn_df = dyn_df.rename(columns={"number": number_col})
        dyn_df[number_col] = dyn_df[number_col].astype(str).str.strip()
        df[number_col] = df[number_col].astype(str).str.strip()
        df = df.merge(dyn_df, on=number_col, how="left", suffixes=("", "_dyn"))
        for col in ["dynCS", "dynCV", "dynEd", "dynAlt"]:
            dyn_col = f"{col}_dyn"
            if dyn_col in df.columns:
                df[col] = df[dyn_col].fillna(df[col])
                df.drop(columns=[dyn_col], inplace=True)
    if "dynamisk" in df.columns:
        for col in ["dynCS", "dynCV", "dynEd", "dynAlt"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        coresv_series = pd.to_numeric(df["coresv"], errors="coerce").fillna(0) if "coresv" in df.columns else 0
        coreva_series = pd.to_numeric(df["corevä"], errors="coerce").fillna(0) if "corevä" in df.columns else 0
        edge_series = pd.to_numeric(df["edge"], errors="coerce").fillna(0) if "edge" in df.columns else 0
        alts_series = pd.to_numeric(df["alts"], errors="coerce").fillna(0) if "alts" in df.columns else 0
        flags_sum = (
            strategi_vals.get("coresv", 0) * coresv_series
            + strategi_vals.get("corevä", 0) * coreva_series
            + strategi_vals.get("edge", 0) * edge_series
            + strategi_vals.get("alts", 0) * alts_series
        )
        fi_value = (
            pd.to_numeric(df["FI"], errors="coerce").fillna(0)
            if "FI" in df.columns
            else 0
        )
        scale = 1 - fi_value
        dyn_off = df["dynamisk"] != 1
        for col in ["dynCS", "dynCV", "dynEd", "dynAlt"]:
            df.loc[dyn_off, col] = 0.0
        for col in ["CS", "CV", "Ed", "Alt"]:
            if col in df.columns:
                df.loc[df["dynamisk"] == 1, col] = 0
        if "coresv" in df.columns:
            df["dynCS"] = df["dynCS"].where(
                df["dynamisk"] != 1,
                df["coresv"].where(df["coresv"] == 1, 0)
                * (strategi_vals.get("coresv", 0) / flags_sum.replace(0, pd.NA))
                * scale,
            )
        if "corevä" in df.columns:
            df["dynCV"] = df["dynCV"].where(
                df["dynamisk"] != 1,
                df["corevä"].where(df["corevä"] == 1, 0)
                * (strategi_vals.get("corevä", 0) / flags_sum.replace(0, pd.NA))
                * scale,
            )
        if "edge" in df.columns:
            df["dynEd"] = df["dynEd"].where(
                df["dynamisk"] != 1,
                df["edge"].where(df["edge"] == 1, 0)
                * (strategi_vals.get("edge", 0) / flags_sum.replace(0, pd.NA))
                * scale,
            )
        if "alts" in df.columns:
            df["dynAlt"] = df["dynAlt"].where(
                df["dynamisk"] != 1,
                df["alts"].where(df["alts"] == 1, 0)
                * (strategi_vals.get("alts", 0) / flags_sum.replace(0, pd.NA))
                * scale,
            )
        dyn_rows = []
        if number_col in df.columns:
            for _, row in df.iterrows():
                number_val = str(row.get(number_col, "")).strip()
                if not number_val:
                    continue
                dyn_rows.append(
                    {
                        "number": number_val,
                        "dynCS": row.get("dynCS", ""),
                        "dynCV": row.get("dynCV", ""),
                        "dynEd": row.get("dynEd", ""),
                        "dynAlt": row.get("dynAlt", ""),
                    }
                )
        _save_mandat_dyn(dyn_rows)
    number_suggestions = []
    if number_col in df.columns:
        number_suggestions = (
            df[number_col]
            .dropna()
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )
        number_suggestions = sorted(
            number_suggestions,
            key=lambda v: (float(v) if str(v).replace(".", "", 1).isdigit() else float("inf"), str(v)),
        )
    if number and number_col in df.columns:
        df = df[df[number_col].astype(str).str.strip() == number]
    df = df.where(pd.notna(df), "")
    preferred = [
        number_col,
        "Kund",
        "FI",
        "dynamisk",
        "coresv",
        "corevä",
        "edge",
        "alts",
    ]
    move_right = [
        "Mandat",
        "Rådgivare",
        "Mandatnotering",
        "Förvaltningsnotering",
        "FI-notering",
        "Godkännande",
        "Placeringsriktlinjer",
        "RG7>25",
        "20%",
        "Akt>75",
        "Akt>25",
        "Alt>50",
        "Rä != 0",
        "Alt!= 0",
    ]
    tail = ["CS", "CV", "Ed", "Alt"]
    existing = [c for c in preferred if c in df.columns]
    remaining = [c for c in df.columns if c not in existing and c not in tail and c not in move_right]
    move_right_existing = [c for c in move_right if c in df.columns]
    tail_existing = [c for c in tail if c in df.columns]
    columns = existing + remaining + tail_existing + move_right_existing
    columns = [c for c in columns if c not in {"dynCS", "dynCV", "dynEd", "dynAlt"}]
    if sort_by == "number" and number_col in df.columns:
        df = df.sort_values(by=number_col, key=lambda s: pd.to_numeric(s, errors="coerce"))
    elif sort_by == "fi" and "FI" in df.columns:
        df = df.sort_values(by="FI", ascending=False, key=lambda s: pd.to_numeric(s, errors="coerce"))

    rows = df.to_dict(orient="records")
    for row in rows:
        row["_row_key"] = _normalize_number_value(row.get(number_col, ""))

    compliance_rows = _build_compliance_rows(rows, number_col) if compliance else []

    return templates.TemplateResponse(
        "mandat.html",
        {
            "request": request,
            "q": q,
            "columns": columns,
            "rows": rows,
            "format_cell": format_cell,
            "number_suggestions": number_suggestions,
            "sort_by": sort_by,
            "compliance": bool(compliance),
            "compliance_rows": compliance_rows,
        },
    )


@app.get("/core-sverige", response_class=HTMLResponse)
def core_sverige(request: Request, ticker: str = ""):
    price = None
    error = ""
    if ticker:
        if yf is None:
            error = "yfinance is not installed"
        else:
            try:
                t = yf.Ticker(ticker)
                fast = getattr(t, "fast_info", {}) or {}
                price = fast.get("last_price") or fast.get("lastPrice")
                if price is None:
                    hist = t.history(period="1d")
                    if not hist.empty:
                        price = float(hist["Close"].iloc[-1])
                if price is None:
                    error = "No price data returned."
            except Exception:
                error = "Failed to fetch price."
    core_data = _load_sheet_from_db("coresvdata")
    if core_data.empty:
        core_data = _load_sheet_from_db("CoreSvData")
    core_actions = _load_sheet_from_db("coresvactions")
    if core_actions.empty:
        core_actions = _load_sheet_from_db("CoreSvActions")
    performance_rows = []
    performance_years = [2022, 2023, 2024, 2025]
    chart_points = []
    if not core_data.empty and "Datum" in core_data.columns:
        perf_df = core_data.copy()
        perf_df["Datum"] = _parse_date_series(perf_df["Datum"])
        perf_df = perf_df.dropna(subset=["Datum"])
        perf_df["Year"] = perf_df["Datum"].dt.year
        perf_df = perf_df.sort_values(by="Datum")
        for label in ["CoreSverige", "OMXS30", "OMXSPI"]:
            if label not in perf_df.columns:
                continue
            row = {"Name": label}
            series = pd.to_numeric(perf_df[label], errors="coerce")
            for year in performance_years:
                year_mask = perf_df["Year"] == year
                if not year_mask.any():
                    row[year] = None
                    continue
                values = series[year_mask]
                last_val = values.dropna().iloc[-1] if not values.dropna().empty else None
                prev_mask = perf_df["Year"] == (year - 1)
                prev_values = series[prev_mask]
                prev_last = prev_values.dropna().iloc[-1] if not prev_values.dropna().empty else None
                if last_val is None or prev_last is None or prev_last == 0:
                    row[year] = None
                else:
                    row[year] = (last_val / prev_last) - 1
            latest_year = perf_df["Year"].max() if not perf_df.empty else None
            if pd.notna(latest_year):
                latest_year = int(latest_year)
                cur_vals = series[perf_df["Year"] == latest_year]
                prev_vals = series[perf_df["Year"] == (latest_year - 1)]
                cur_last = cur_vals.dropna().iloc[-1] if not cur_vals.dropna().empty else None
                prev_last = prev_vals.dropna().iloc[-1] if not prev_vals.dropna().empty else None
                if cur_last is None or prev_last is None or prev_last == 0:
                    row["YTD"] = None
                else:
                    row["YTD"] = (cur_last / prev_last) - 1
            else:
                row["YTD"] = None
            performance_rows.append(row)
        # raw chart points for client-side range normalization
        for _, row in perf_df.iterrows():
            point = {"date": row["Datum"].strftime("%Y-%m-%d")}
            for label in ["CoreSverige", "OMXS30", "OMXSPI"]:
                if label in perf_df.columns:
                    point[label] = _to_float(row.get(label))
            chart_points.append(point)
    holdings_rows = []
    kurs_by_verdepapper = {}
    try:
        taggar_df = _load_taggar_table()
    except Exception:
        taggar_df = pd.DataFrame()
    currency_kurs_map = {}
    if not taggar_df.empty:
        if "Short Name" in taggar_df.columns and "Kurs" in taggar_df.columns:
            currency_map = (
                taggar_df[["Short Name", "Kurs"]]
                .dropna()
                .assign(short=lambda d: d["Short Name"].astype(str).str.strip())
            )
            currency_map = currency_map[currency_map["short"] != ""]
            currency_kurs_map = dict(
                zip(currency_map["short"], currency_map["Kurs"].apply(_to_float))
            )
    if not taggar_df.empty and "Modellnamn" in taggar_df.columns:
        kurs_by_verdepapper = {}
        fx_by_verdepapper = {}
        sektor_by_verdepapper = {}
        if "Kurs" in taggar_df.columns:
            kurs_map = (
                taggar_df[["Modellnamn", "Kurs"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            kurs_map = kurs_map[kurs_map["modell"] != ""]
            kurs_by_verdepapper = dict(
                zip(
                    kurs_map["modell"].str.casefold(),
                    kurs_map["Kurs"].apply(_to_float),
                )
            )
        if "FX" in taggar_df.columns:
            fx_map = (
                taggar_df[["Modellnamn", "FX"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            fx_map = fx_map[fx_map["modell"] != ""]
            fx_by_verdepapper = dict(
                zip(fx_map["modell"].str.casefold(), fx_map["FX"])
            )
        if "Sektor" in taggar_df.columns:
            sektor_map = (
                taggar_df[["Modellnamn", "Sektor"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            sektor_map = sektor_map[sektor_map["modell"] != ""]
            sektor_by_verdepapper = dict(
                zip(sektor_map["modell"].str.casefold(), sektor_map["Sektor"])
            )
    else:
        fx_by_verdepapper = {}
        sektor_by_verdepapper = {}
    gav_by_verdepapper = {}
    if not core_actions.empty and "Värdepapper" in core_actions.columns and "Antal" in core_actions.columns:
        actions = core_actions.copy()
        actions["Värdepapper"] = actions["Värdepapper"].astype(str).str.strip()
        actions["Värdepapper_norm"] = actions["Värdepapper"].str.casefold()
        actions["Antal"] = _to_float_series(actions["Antal"]).fillna(0)
        if "Datum" in actions.columns:
            actions["Datum"] = pd.to_datetime(actions["Datum"], errors="coerce")
            actions = actions.sort_values(by="Datum")
        if "Kurs" in actions.columns:
            actions["Kurs"] = _to_float_series(actions["Kurs"]).fillna(0)
        nettokassa = _latest_nettokassa(actions)
        # GAV per holding: reset after position goes to 0
        for name, group in actions.groupby("Värdepapper_norm"):
            position = 0.0
            cost = 0.0
            for _, row in group.iterrows():
                qty = _to_float(row.get("Antal", 0)) or 0
                price = _to_float(row.get("Kurs", 0)) or 0
                if qty > 0:
                    cost += qty * price
                    position += qty
                elif qty < 0 and position > 0:
                    sell_qty = min(position, abs(qty))
                    avg_cost = cost / position if position else 0
                    cost -= avg_cost * sell_qty
                    position -= sell_qty
                if position <= 0:
                    position = 0.0
                    cost = 0.0
            gav_by_verdepapper[name] = (cost / position) if position else 0
        holdings = (
            actions.groupby("Värdepapper_norm", dropna=False)["Antal"]
            .sum()
            .reset_index()
        )
        name_map = (
            actions.dropna(subset=["Värdepapper_norm"])
            .groupby("Värdepapper_norm")["Värdepapper"]
            .first()
            .to_dict()
        )
        holdings = holdings[holdings["Värdepapper_norm"].astype(str).str.strip() != ""]
        holdings = holdings[holdings["Antal"] != 0]
        holdings["Värdepapper"] = holdings["Värdepapper_norm"].map(name_map).fillna(holdings["Värdepapper_norm"])
        holdings_rows = holdings.to_dict(orient="records")
        if nettokassa is not None:
            holdings_rows = [
                r
                for r in holdings_rows
                if str(r.get("Värdepapper", "")).strip().upper() not in {"KASSA", "SEK"}
            ]
            holdings_rows.append({"Värdepapper": "Kassa", "Antal": nettokassa})
        for row in holdings_rows:
            name = str(row.get("Värdepapper", "")).strip()
            name_norm = name.casefold()
            if name.upper() in {"KASSA", "SEK"}:
                row["Värdepapper"] = "Kassa"
                row["Kurs"] = 1
                row["FX"] = "SEK"
                row["Sektor"] = ""
                row["Värde"] = _to_float(row.get("Antal", 0)) or 0
            else:
                kurs = kurs_by_verdepapper.get(name_norm, "")
                row["Kurs"] = kurs
                fx_code = fx_by_verdepapper.get(name_norm, "")
                row["FX"] = fx_code
                row["Sektor"] = sektor_by_verdepapper.get(name_norm, "")
                fx_rate = _to_float(currency_kurs_map.get(str(fx_code).strip(), 1)) or 1
                if kurs not in ("", None):
                    value = (row.get("Antal", 0) or 0) * kurs * fx_rate
                else:
                    value = 0
                row["Värde"] = value
        total_value = sum((_to_float(r.get("Värde", 0)) or 0) for r in holdings_rows)
        for row in holdings_rows:
            val = _to_float(row.get("Värde", 0)) or 0
            row["Vikt"] = (val / total_value) if total_value else 0
            row["GAV"] = gav_by_verdepapper.get(str(row.get("Värdepapper", "")).strip().casefold(), 0)
            gav_val = _to_float(row.get("GAV", 0)) or 0
            kurs_val = _to_float(row.get("Kurs", 0)) or 0
            fx_code = str(row.get("FX", "")).strip()
            fx_rate = _to_float(currency_kurs_map.get(fx_code, 1)) or 1
            kurs_val_adj = (
                kurs_val * fx_rate
                if fx_code.upper() not in {"", "SEK"}
                else kurs_val
            )
            row["Utv"] = (kurs_val_adj / gav_val - 1) if gav_val else 0
        # sort by Vikt desc, keep Kassa last
        sek_rows = [r for r in holdings_rows if str(r.get("Värdepapper", "")).strip().upper() == "KASSA"]
        other_rows = [r for r in holdings_rows if str(r.get("Värdepapper", "")).strip().upper() != "KASSA"]
        other_rows = sorted(other_rows, key=lambda r: _to_float(r.get("Vikt", 0)) or 0, reverse=True)
        holdings_rows = other_rows + sek_rows
    sector_totals = {}
    utv_points = []
    if holdings_rows:
        for row in holdings_rows:
            if str(row.get("Värdepapper", "")).strip().upper() == "KASSA":
                continue
            sektor = str(row.get("Sektor", "")).strip()
            if not sektor:
                continue
            sector_totals[sektor] = sector_totals.get(sektor, 0) + (_to_float(row.get("Värde", 0)) or 0)
        for row in holdings_rows:
            name = str(row.get("Värdepapper", "")).strip()
            if name.upper() == "KASSA":
                continue
            utv_val = _to_float(row.get("Utv", 0)) or 0
            utv_points.append((name, utv_val))
        utv_points = sorted(utv_points, key=lambda x: x[1], reverse=True)
    if not core_data.empty and "Datum" in core_data.columns:
        core_data = core_data.copy()
        core_data["__date"] = _parse_date_series(core_data["Datum"])
        core_data = core_data.sort_values(by="__date", ascending=False).drop(columns=["__date"])
    if not core_actions.empty:
        core_actions = _ensure_row_id(core_actions, "coresvactions")
    if not core_actions.empty and "Datum" in core_actions.columns:
        core_actions = core_actions.copy()
        core_actions["__date"] = _parse_date_series(core_actions["Datum"])
        core_actions = core_actions.sort_values(by="__date", ascending=False).drop(columns=["__date"])
    data_cols = core_data.columns.tolist() if not core_data.empty else []
    action_cols = [c for c in core_actions.columns.tolist() if c != "row_id"] if not core_actions.empty else []
    data_rows = _safe_rows(core_data) if not core_data.empty else []
    action_rows = _safe_rows(core_actions) if not core_actions.empty else []
    return templates.TemplateResponse(
        "core_sverige.html",
        {
            "request": request,
            "ticker": ticker,
            "price": price,
            "error": error,
            "data_cols": data_cols,
            "data_rows": data_rows,
            "action_cols": action_cols,
            "action_rows": action_rows,
            "holdings_rows": holdings_rows,
            "holdings_total_value": total_value if "total_value" in locals() else 0,
            "performance_rows": performance_rows,
            "performance_years": performance_years,
            "chart_points": json.dumps(chart_points),
            "sector_totals": json.dumps(sector_totals),
            "utv_points": json.dumps(utv_points),
            "format_cell": format_cell,
            "format_percent": format_percent,
            "format_percent_1": format_percent_1,
        },
    )


@app.get("/edge", response_class=HTMLResponse)
def edge(request: Request):
    edge_data = _load_sheet_from_db("edgedata")
    if edge_data.empty:
        edge_data = _load_sheet_from_db("EdgeData")
    edge_actions = _load_sheet_from_db("edgeactions")
    if edge_actions.empty:
        edge_actions = _load_sheet_from_db("EdgeActions")
    performance_rows = []
    performance_years = [2022, 2023, 2024, 2025]
    chart_points = []
    if not edge_data.empty and "Datum" in edge_data.columns:
        perf_df = edge_data.copy()
        perf_df["Datum"] = _parse_date_series(perf_df["Datum"])
        perf_df = perf_df.dropna(subset=["Datum"])
        perf_df["Year"] = perf_df["Datum"].dt.year
        perf_df = perf_df.sort_values(by="Datum")
        for label in ["Edge", "FirstNorth", "OMXSSCPI"]:
            if label not in perf_df.columns:
                continue
            row = {"Name": label}
            series = pd.to_numeric(perf_df[label], errors="coerce")
            for year in performance_years:
                year_mask = perf_df["Year"] == year
                if not year_mask.any():
                    row[year] = None
                    continue
                values = series[year_mask]
                last_val = values.dropna().iloc[-1] if not values.dropna().empty else None
                prev_mask = perf_df["Year"] == (year - 1)
                prev_values = series[prev_mask]
                prev_last = prev_values.dropna().iloc[-1] if not prev_values.dropna().empty else None
                if last_val is None or prev_last is None or prev_last == 0:
                    row[year] = None
                else:
                    row[year] = (last_val / prev_last) - 1
            latest_year = perf_df["Year"].max() if not perf_df.empty else None
            if pd.notna(latest_year):
                latest_year = int(latest_year)
                cur_vals = series[perf_df["Year"] == latest_year]
                prev_vals = series[perf_df["Year"] == (latest_year - 1)]
                cur_last = cur_vals.dropna().iloc[-1] if not cur_vals.dropna().empty else None
                prev_last = prev_vals.dropna().iloc[-1] if not prev_vals.dropna().empty else None
                if cur_last is None or prev_last is None or prev_last == 0:
                    row["YTD"] = None
                else:
                    row["YTD"] = (cur_last / prev_last) - 1
            else:
                row["YTD"] = None
            performance_rows.append(row)
        for _, row in perf_df.iterrows():
            point = {"date": row["Datum"].strftime("%Y-%m-%d")}
            for label in ["Edge", "FirstNorth", "OMXSSCPI"]:
                if label in perf_df.columns:
                    point[label] = _to_float(row.get(label))
            chart_points.append(point)

    holdings_rows = []
    kurs_by_verdepapper = {}
    gav_by_verdepapper = {}
    try:
        taggar_df = _load_taggar_table()
    except Exception:
        taggar_df = pd.DataFrame()
    currency_kurs_map = {}
    if not taggar_df.empty:
        if "Short Name" in taggar_df.columns and "Kurs" in taggar_df.columns:
            currency_map = (
                taggar_df[["Short Name", "Kurs"]]
                .dropna()
                .assign(short=lambda d: d["Short Name"].astype(str).str.strip())
            )
            currency_map = currency_map[currency_map["short"] != ""]
            currency_kurs_map = dict(
                zip(currency_map["short"], currency_map["Kurs"].apply(_to_float))
            )
    fx_by_verdepapper = {}
    if not taggar_df.empty and "Modellnamn" in taggar_df.columns:
        sektor_by_verdepapper = {}
        if "Kurs" in taggar_df.columns:
            kurs_map = (
                taggar_df[["Modellnamn", "Kurs"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            kurs_map = kurs_map[kurs_map["modell"] != ""]
            kurs_by_verdepapper = dict(
                zip(
                    kurs_map["modell"].str.casefold(),
                    kurs_map["Kurs"].apply(_to_float),
                )
            )
        if "FX" in taggar_df.columns:
            fx_map = (
                taggar_df[["Modellnamn", "FX"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            fx_map = fx_map[fx_map["modell"] != ""]
            fx_by_verdepapper = dict(
                zip(fx_map["modell"].str.casefold(), fx_map["FX"])
            )
        if "Sektor" in taggar_df.columns:
            sektor_map = (
                taggar_df[["Modellnamn", "Sektor"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            sektor_map = sektor_map[sektor_map["modell"] != ""]
            sektor_by_verdepapper = dict(
                zip(sektor_map["modell"].str.casefold(), sektor_map["Sektor"])
            )
    else:
        sektor_by_verdepapper = {}

    if not edge_actions.empty and "Värdepapper" in edge_actions.columns and "Antal" in edge_actions.columns:
        actions = edge_actions.copy()
        actions["Värdepapper"] = actions["Värdepapper"].astype(str).str.strip()
        actions["Värdepapper_norm"] = actions["Värdepapper"].str.casefold()
        actions["Antal"] = _to_float_series(actions["Antal"]).fillna(0)
        if "Datum" in actions.columns:
            actions["Datum"] = pd.to_datetime(actions["Datum"], errors="coerce")
            actions = actions.sort_values(by="Datum")
        if "Kurs" in actions.columns:
            actions["Kurs"] = _to_float_series(actions["Kurs"]).fillna(0)
        nettokassa = _latest_nettokassa(actions)
        for name, group in actions.groupby("Värdepapper_norm"):
            position = 0.0
            cost = 0.0
            for _, row in group.iterrows():
                qty = _to_float(row.get("Antal", 0)) or 0
                price = _to_float(row.get("Kurs", 0)) or 0
                if qty > 0:
                    cost += qty * price
                    position += qty
                elif qty < 0 and position > 0:
                    sell_qty = min(position, abs(qty))
                    avg_cost = cost / position if position else 0
                    cost -= avg_cost * sell_qty
                    position -= sell_qty
                if position <= 0:
                    position = 0.0
                    cost = 0.0
            gav_by_verdepapper[name] = (cost / position) if position else 0
        holdings = (
            actions.groupby("Värdepapper_norm", dropna=False)["Antal"]
            .sum()
            .reset_index()
        )
        name_map = (
            actions.dropna(subset=["Värdepapper_norm"])
            .groupby("Värdepapper_norm")["Värdepapper"]
            .first()
            .to_dict()
        )
        holdings = holdings[holdings["Värdepapper_norm"].astype(str).str.strip() != ""]
        holdings = holdings[holdings["Antal"] != 0]
        holdings["Värdepapper"] = holdings["Värdepapper_norm"].map(name_map).fillna(holdings["Värdepapper_norm"])
        holdings_rows = holdings.to_dict(orient="records")
        if nettokassa is not None:
            holdings_rows = [
                r
                for r in holdings_rows
                if str(r.get("Värdepapper", "")).strip().upper() not in {"KASSA", "SEK"}
            ]
            holdings_rows.append({"Värdepapper": "Kassa", "Antal": nettokassa})
        for row in holdings_rows:
            name = str(row.get("Värdepapper", "")).strip()
            name_norm = name.casefold()
            if name.upper() in {"KASSA", "SEK"}:
                row["Värdepapper"] = "Kassa"
                row["Kurs"] = 1
                row["FX"] = "SEK"
                row["Sektor"] = ""
                row["Värde"] = _to_float(row.get("Antal", 0)) or 0
            else:
                kurs = kurs_by_verdepapper.get(name_norm, "")
                row["Kurs"] = kurs
                fx_code = fx_by_verdepapper.get(name_norm, "")
                row["FX"] = fx_code
                row["Sektor"] = sektor_by_verdepapper.get(name_norm, "")
                fx_rate = _to_float(currency_kurs_map.get(str(fx_code).strip(), 1)) or 1
                if kurs not in ("", None):
                    value = (row.get("Antal", 0) or 0) * kurs * fx_rate
                else:
                    value = 0
                row["Värde"] = value
        total_value = sum((_to_float(r.get("Värde", 0)) or 0) for r in holdings_rows)
        for row in holdings_rows:
            val = _to_float(row.get("Värde", 0)) or 0
            row["Vikt"] = (val / total_value) if total_value else 0
            row["GAV"] = gav_by_verdepapper.get(str(row.get("Värdepapper", "")).strip().casefold(), 0)
            gav_val = _to_float(row.get("GAV", 0)) or 0
            kurs_val = _to_float(row.get("Kurs", 0)) or 0
            fx_code = str(row.get("FX", "")).strip()
            fx_rate = _to_float(currency_kurs_map.get(fx_code, 1)) or 1
            kurs_val_adj = (
                kurs_val * fx_rate
                if fx_code.upper() not in {"", "SEK"}
                else kurs_val
            )
            row["Utv"] = (kurs_val_adj / gav_val - 1) if gav_val else 0
        sek_rows = [r for r in holdings_rows if str(r.get("Värdepapper", "")).strip().upper() == "KASSA"]
        other_rows = [r for r in holdings_rows if str(r.get("Värdepapper", "")).strip().upper() != "KASSA"]
        other_rows = sorted(other_rows, key=lambda r: _to_float(r.get("Vikt", 0)) or 0, reverse=True)
        holdings_rows = other_rows + sek_rows
    sector_totals = {}
    utv_points = []
    if holdings_rows:
        for row in holdings_rows:
            if str(row.get("Värdepapper", "")).strip().upper() == "KASSA":
                continue
            sektor = str(row.get("Sektor", "")).strip()
            if not sektor:
                continue
            sector_totals[sektor] = sector_totals.get(sektor, 0) + (_to_float(row.get("Värde", 0)) or 0)
        for row in holdings_rows:
            name = str(row.get("Värdepapper", "")).strip()
            if name.upper() == "KASSA":
                continue
            utv_val = _to_float(row.get("Utv", 0)) or 0
            utv_points.append((name, utv_val))
        utv_points = sorted(utv_points, key=lambda x: x[1], reverse=True)
    if not edge_data.empty and "Datum" in edge_data.columns:
        edge_data = edge_data.copy()
        edge_data["__date"] = _parse_date_series(edge_data["Datum"])
        edge_data = edge_data.sort_values(by="__date", ascending=False).drop(columns=["__date"])
    if not edge_actions.empty:
        edge_actions = _ensure_row_id(edge_actions, "edgeactions")
    if not edge_actions.empty and "Datum" in edge_actions.columns:
        edge_actions = edge_actions.copy()
        edge_actions["__date"] = _parse_date_series(edge_actions["Datum"])
        edge_actions = edge_actions.sort_values(by="__date", ascending=False).drop(columns=["__date"])
    data_cols = edge_data.columns.tolist() if not edge_data.empty else []
    action_cols = [c for c in edge_actions.columns.tolist() if c != "row_id"] if not edge_actions.empty else []
    data_rows = _safe_rows(edge_data) if not edge_data.empty else []
    action_rows = _safe_rows(edge_actions) if not edge_actions.empty else []
    return templates.TemplateResponse(
        "edge.html",
        {
            "request": request,
            "data_cols": data_cols,
            "data_rows": data_rows,
            "action_cols": action_cols,
            "action_rows": action_rows,
            "holdings_rows": holdings_rows,
            "holdings_total_value": total_value if "total_value" in locals() else 0,
            "performance_rows": performance_rows,
            "performance_years": performance_years,
            "chart_points": json.dumps(chart_points),
            "sector_totals": json.dumps(sector_totals),
            "utv_points": json.dumps(utv_points),
            "format_cell": format_cell,
            "format_percent": format_percent,
            "format_percent_1": format_percent_1,
        },
    )


@app.post("/model-actions-add")
async def model_actions_add(request: Request):
    form = await request.form()
    model = (form.get("model") or "").strip().lower()
    table_map = {
        "core-sverige": "coresvactions",
        "edge": "edgeactions",
        "alternativa": "altactions",
        "core-varlden": "corevactions",
    }
    table = table_map.get(model)
    if not table:
        return RedirectResponse(url="/", status_code=303)
    payload = {
        "Datum": form.get("Datum", ""),
        "Värdepapper": form.get("Värdepapper", ""),
        "Transaktionstyp": form.get("Transaktionstyp", ""),
        "Antal": form.get("Antal", ""),
        "Kurs": form.get("Kurs", ""),
    }
    _append_model_action(table, payload)
    return RedirectResponse(url=f"/{model}", status_code=303)


@app.post("/model-actions-save")
async def model_actions_save(request: Request):
    form = await request.form()
    model = (form.get("model") or "").strip().lower()
    row_id = form.get("row_id")
    table_map = {
        "core-sverige": "coresvactions",
        "edge": "edgeactions",
        "alternativa": "altactions",
        "core-varlden": "corevactions",
    }
    table = table_map.get(model)
    if not table or not row_id:
        return RedirectResponse(request.headers.get("referer", "/"), status_code=303)
    df = _load_sheet_from_db(table)
    if df.empty:
        return RedirectResponse(request.headers.get("referer", f"/{model}"), status_code=303)
    df = _ensure_row_id(df, table)
    df["row_id"] = df["row_id"].astype(str)
    mask = df["row_id"] == str(row_id)
    if not mask.any():
        return RedirectResponse(request.headers.get("referer", f"/{model}"), status_code=303)
    edited_date = None
    if "Datum" in df.columns:
        try:
            edited_date = df.loc[mask, "Datum"].iloc[0]
        except Exception:
            edited_date = None
    editable = ["Datum", "Värdepapper", "Transaktionstyp", "Antal", "Kurs"]
    for col in editable:
        key = f"row__{row_id}__{col}"
        if key in form:
            df.loc[mask, col] = _coerce_cell_for_column(df, col, form.get(key))
    if "Datum" in df.columns:
        try:
            new_date = df.loc[mask, "Datum"].iloc[0]
            if new_date is not None and str(new_date).strip() != "":
                edited_date = new_date
        except Exception:
            pass
    if edited_date is not None:
        df = _recalc_kassa_from_date(df, edited_date)
    else:
        df = _recalc_kassa(df)
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(table, conn, if_exists="replace", index=False)
    return RedirectResponse(request.headers.get("referer", f"/{model}"), status_code=303)


@app.post("/model-actions-delete")
async def model_actions_delete(request: Request):
    form = await request.form()
    model = (form.get("model") or "").strip().lower()
    row_id = form.get("row_id")
    table_map = {
        "core-sverige": "coresvactions",
        "edge": "edgeactions",
        "alternativa": "altactions",
        "core-varlden": "corevactions",
    }
    table = table_map.get(model)
    if not table or not row_id:
        return RedirectResponse(request.headers.get("referer", "/"), status_code=303)
    df = _load_sheet_from_db(table)
    if df.empty:
        return RedirectResponse(request.headers.get("referer", f"/{model}"), status_code=303)
    df = _ensure_row_id(df, table)
    deleted_date = None
    if "Datum" in df.columns:
        try:
            deleted_date = df.loc[df["row_id"].astype(str) == str(row_id), "Datum"].iloc[0]
        except Exception:
            deleted_date = None
    df["row_id"] = df["row_id"].astype(str)
    df = df[df["row_id"] != str(row_id)]
    if deleted_date is not None:
        df = _recalc_kassa_from_date(df, deleted_date)
    else:
        df = _recalc_kassa(df)
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(table, conn, if_exists="replace", index=False)
    return RedirectResponse(request.headers.get("referer", f"/{model}"), status_code=303)


@app.get("/alternativa", response_class=HTMLResponse)
def alternativa(request: Request):
    alt_data = _load_sheet_from_db("altdata")
    if alt_data.empty:
        alt_data = _load_sheet_from_db("AltData")
    alt_actions = _load_sheet_from_db("altactions")
    if alt_actions.empty:
        alt_actions = _load_sheet_from_db("AltActions")
    performance_rows = []
    performance_years = [2022, 2023, 2024, 2025]
    chart_points = []
    if not alt_data.empty and "Datum" in alt_data.columns:
        perf_df = alt_data.copy()
        perf_df["Datum"] = _parse_date_series(perf_df["Datum"])
        perf_df = perf_df.dropna(subset=["Datum"])
        perf_df["Year"] = perf_df["Datum"].dt.year
        perf_df = perf_df.sort_values(by="Datum")
        for label in ["Alternativa", "RLY SEK"]:
            if label not in perf_df.columns:
                continue
            row = {"Name": label}
            series = pd.to_numeric(perf_df[label], errors="coerce")
            for year in performance_years:
                year_mask = perf_df["Year"] == year
                if not year_mask.any():
                    row[year] = None
                    continue
                values = series[year_mask]
                last_val = values.dropna().iloc[-1] if not values.dropna().empty else None
                prev_mask = perf_df["Year"] == (year - 1)
                prev_values = series[prev_mask]
                prev_last = prev_values.dropna().iloc[-1] if not prev_values.dropna().empty else None
                if last_val is None or prev_last is None or prev_last == 0:
                    row[year] = None
                else:
                    row[year] = (last_val / prev_last) - 1
            latest_year = perf_df["Year"].max() if not perf_df.empty else None
            if pd.notna(latest_year):
                latest_year = int(latest_year)
                cur_vals = series[perf_df["Year"] == latest_year]
                prev_vals = series[perf_df["Year"] == (latest_year - 1)]
                cur_last = cur_vals.dropna().iloc[-1] if not cur_vals.dropna().empty else None
                prev_last = prev_vals.dropna().iloc[-1] if not prev_vals.dropna().empty else None
                if cur_last is None or prev_last is None or prev_last == 0:
                    row["YTD"] = None
                else:
                    row["YTD"] = (cur_last / prev_last) - 1
            else:
                row["YTD"] = None
            performance_rows.append(row)
        for _, row in perf_df.iterrows():
            point = {"date": row["Datum"].strftime("%Y-%m-%d")}
            for label in ["Alternativa", "RLY SEK"]:
                if label in perf_df.columns:
                    point[label] = _to_float(row.get(label))
            chart_points.append(point)

    holdings_rows = []
    kurs_by_verdepapper = {}
    gav_by_verdepapper = {}
    try:
        taggar_df = _load_taggar_table()
    except Exception:
        taggar_df = pd.DataFrame()
    currency_kurs_map = {}
    if not taggar_df.empty:
        if "Short Name" in taggar_df.columns and "Kurs" in taggar_df.columns:
            currency_map = (
                taggar_df[["Short Name", "Kurs"]]
                .dropna()
                .assign(short=lambda d: d["Short Name"].astype(str).str.strip())
            )
            currency_map = currency_map[currency_map["short"] != ""]
            currency_kurs_map = dict(
                zip(currency_map["short"], currency_map["Kurs"].apply(_to_float))
            )
    fx_by_verdepapper = {}
    sektor_by_verdepapper = {}
    if not taggar_df.empty and "Modellnamn" in taggar_df.columns:
        if "Kurs" in taggar_df.columns:
            kurs_map = (
                taggar_df[["Modellnamn", "Kurs"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            kurs_map = kurs_map[kurs_map["modell"] != ""]
            kurs_by_verdepapper = dict(
                zip(
                    kurs_map["modell"].str.casefold(),
                    kurs_map["Kurs"].apply(_to_float),
                )
            )
        if "FX" in taggar_df.columns:
            fx_map = (
                taggar_df[["Modellnamn", "FX"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            fx_map = fx_map[fx_map["modell"] != ""]
            fx_by_verdepapper = dict(
                zip(fx_map["modell"].str.casefold(), fx_map["FX"])
            )
        if "Sektor" in taggar_df.columns:
            sektor_map = (
                taggar_df[["Modellnamn", "Sektor"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            sektor_map = sektor_map[sektor_map["modell"] != ""]
            sektor_by_verdepapper = dict(
                zip(sektor_map["modell"].str.casefold(), sektor_map["Sektor"])
            )

    if not alt_actions.empty and "Värdepapper" in alt_actions.columns and "Antal" in alt_actions.columns:
        actions = alt_actions.copy()
        actions["Värdepapper"] = actions["Värdepapper"].astype(str).str.strip()
        actions["Värdepapper_norm"] = actions["Värdepapper"].str.casefold()
        actions["Antal"] = _to_float_series(actions["Antal"]).fillna(0)
        if "Datum" in actions.columns:
            actions["Datum"] = pd.to_datetime(actions["Datum"], errors="coerce")
            actions = actions.sort_values(by="Datum")
        if "Kurs" in actions.columns:
            actions["Kurs"] = _to_float_series(actions["Kurs"]).fillna(0)
        nettokassa = _latest_nettokassa(actions)
        for name, group in actions.groupby("Värdepapper_norm"):
            position = 0.0
            cost = 0.0
            for _, row in group.iterrows():
                qty = _to_float(row.get("Antal", 0)) or 0
                price = _to_float(row.get("Kurs", 0)) or 0
                if qty > 0:
                    cost += qty * price
                    position += qty
                elif qty < 0 and position > 0:
                    sell_qty = min(position, abs(qty))
                    avg_cost = cost / position if position else 0
                    cost -= avg_cost * sell_qty
                    position -= sell_qty
                if position <= 0:
                    position = 0.0
                    cost = 0.0
            gav_by_verdepapper[name] = (cost / position) if position else 0
        holdings = (
            actions.groupby("Värdepapper_norm", dropna=False)["Antal"]
            .sum()
            .reset_index()
        )
        name_map = (
            actions.dropna(subset=["Värdepapper_norm"])
            .groupby("Värdepapper_norm")["Värdepapper"]
            .first()
            .to_dict()
        )
        holdings = holdings[holdings["Värdepapper_norm"].astype(str).str.strip() != ""]
        holdings = holdings[holdings["Antal"] != 0]
        holdings["Värdepapper"] = holdings["Värdepapper_norm"].map(name_map).fillna(holdings["Värdepapper_norm"])
        holdings_rows = holdings.to_dict(orient="records")
        if nettokassa is not None:
            holdings_rows = [
                r
                for r in holdings_rows
                if str(r.get("Värdepapper", "")).strip().upper() not in {"KASSA", "SEK"}
            ]
            holdings_rows.append({"Värdepapper": "Kassa", "Antal": nettokassa})
        for row in holdings_rows:
            name = str(row.get("Värdepapper", "")).strip()
            name_norm = name.casefold()
            if name.upper() in {"KASSA", "SEK"}:
                row["Värdepapper"] = "Kassa"
                row["Kurs"] = 1
                row["FX"] = "SEK"
                row["Sektor"] = ""
                row["Värde"] = _to_float(row.get("Antal", 0)) or 0
            else:
                kurs = kurs_by_verdepapper.get(name_norm, "")
                row["Kurs"] = kurs
                fx_code = fx_by_verdepapper.get(name_norm, "")
                row["FX"] = fx_code
                row["Sektor"] = sektor_by_verdepapper.get(name_norm, "")
                fx_rate = _to_float(currency_kurs_map.get(str(fx_code).strip(), 1)) or 1
                if kurs not in ("", None):
                    value = (row.get("Antal", 0) or 0) * kurs * fx_rate
                else:
                    value = 0
                row["Värde"] = value
        total_value = sum((_to_float(r.get("Värde", 0)) or 0) for r in holdings_rows)
        for row in holdings_rows:
            val = _to_float(row.get("Värde", 0)) or 0
            row["Vikt"] = (val / total_value) if total_value else 0
            row["GAV"] = gav_by_verdepapper.get(str(row.get("Värdepapper", "")).strip().casefold(), 0)
            gav_val = _to_float(row.get("GAV", 0)) or 0
            kurs_val = _to_float(row.get("Kurs", 0)) or 0
            fx_code = str(row.get("FX", "")).strip()
            fx_rate = _to_float(currency_kurs_map.get(fx_code, 1)) or 1
            kurs_val_adj = (
                kurs_val * fx_rate
                if fx_code.upper() not in {"", "SEK"}
                else kurs_val
            )
            row["Utv"] = (kurs_val_adj / gav_val - 1) if gav_val else 0
        sek_rows = [r for r in holdings_rows if str(r.get("Värdepapper", "")).strip().upper() == "KASSA"]
        other_rows = [r for r in holdings_rows if str(r.get("Värdepapper", "")).strip().upper() != "KASSA"]
        other_rows = sorted(other_rows, key=lambda r: _to_float(r.get("Vikt", 0)) or 0, reverse=True)
        holdings_rows = other_rows + sek_rows
    if not alt_data.empty and "Datum" in alt_data.columns:
        alt_data = alt_data.copy()
        alt_data["__date"] = _parse_date_series(alt_data["Datum"])
        alt_data = alt_data.sort_values(by="__date", ascending=False).drop(columns=["__date"])
    if not alt_actions.empty:
        alt_actions = _ensure_row_id(alt_actions, "altactions")
    if not alt_actions.empty and "Datum" in alt_actions.columns:
        alt_actions = alt_actions.copy()
        alt_actions["__date"] = _parse_date_series(alt_actions["Datum"])
        alt_actions = alt_actions.sort_values(by="__date", ascending=False).drop(columns=["__date"])
    data_cols = alt_data.columns.tolist() if not alt_data.empty else []
    action_cols = [c for c in alt_actions.columns.tolist() if c != "row_id"] if not alt_actions.empty else []
    data_rows = _safe_rows(alt_data) if not alt_data.empty else []
    action_rows = _safe_rows(alt_actions) if not alt_actions.empty else []
    return templates.TemplateResponse(
        "alternativa.html",
        {
            "request": request,
            "data_cols": data_cols,
            "data_rows": data_rows,
            "action_cols": action_cols,
            "action_rows": action_rows,
            "holdings_rows": holdings_rows,
            "holdings_total_value": total_value if "total_value" in locals() else 0,
            "performance_rows": performance_rows,
            "performance_years": performance_years,
            "chart_points": json.dumps(chart_points),
            "format_cell": format_cell,
            "format_percent": format_percent,
            "format_percent_1": format_percent_1,
        },
    )


@app.get("/core-varlden", response_class=HTMLResponse)
def core_varlden(request: Request):
    corev_data = _load_sheet_from_db("corevdata")
    if corev_data.empty:
        corev_data = _load_sheet_from_db("CoreVData")
    corev_actions = _load_sheet_from_db("corevactions")
    if corev_actions.empty:
        corev_actions = _load_sheet_from_db("CoreVActions")
    performance_rows = []
    performance_years = [2022, 2023, 2024, 2025]
    chart_points = []
    if not corev_data.empty and "Datum" in corev_data.columns:
        perf_df = corev_data.copy()
        perf_df["Datum"] = _parse_date_series(perf_df["Datum"])
        perf_df = perf_df.dropna(subset=["Datum"])
        perf_df["Year"] = perf_df["Datum"].dt.year
        perf_df = perf_df.sort_values(by="Datum")
        for label in ["CoreVärlden", "MSCI World SEK"]:
            if label not in perf_df.columns:
                continue
            row = {"Name": label}
            series = pd.to_numeric(perf_df[label], errors="coerce")
            for year in performance_years:
                year_mask = perf_df["Year"] == year
                if not year_mask.any():
                    row[year] = None
                    continue
                values = series[year_mask]
                last_val = values.dropna().iloc[-1] if not values.dropna().empty else None
                prev_mask = perf_df["Year"] == (year - 1)
                prev_values = series[prev_mask]
                prev_last = prev_values.dropna().iloc[-1] if not prev_values.dropna().empty else None
                if last_val is None or prev_last is None or prev_last == 0:
                    row[year] = None
                else:
                    row[year] = (last_val / prev_last) - 1
            latest_year = perf_df["Year"].max() if not perf_df.empty else None
            if pd.notna(latest_year):
                latest_year = int(latest_year)
                cur_vals = series[perf_df["Year"] == latest_year]
                prev_vals = series[perf_df["Year"] == (latest_year - 1)]
                cur_last = cur_vals.dropna().iloc[-1] if not cur_vals.dropna().empty else None
                prev_last = prev_vals.dropna().iloc[-1] if not prev_vals.dropna().empty else None
                if cur_last is None or prev_last is None or prev_last == 0:
                    row["YTD"] = None
                else:
                    row["YTD"] = (cur_last / prev_last) - 1
            else:
                row["YTD"] = None
            performance_rows.append(row)
        for _, row in perf_df.iterrows():
            point = {"date": row["Datum"].strftime("%Y-%m-%d")}
            for label in ["CoreVärlden", "MSCI World SEK"]:
                if label in perf_df.columns:
                    point[label] = _to_float(row.get(label))
            chart_points.append(point)

    holdings_rows = []
    kurs_by_verdepapper = {}
    gav_by_verdepapper = {}
    try:
        taggar_df = _load_taggar_table()
    except Exception:
        taggar_df = pd.DataFrame()
    currency_kurs_map = {}
    if not taggar_df.empty:
        if "Short Name" in taggar_df.columns and "Kurs" in taggar_df.columns:
            currency_map = (
                taggar_df[["Short Name", "Kurs"]]
                .dropna()
                .assign(short=lambda d: d["Short Name"].astype(str).str.strip())
            )
            currency_map = currency_map[currency_map["short"] != ""]
            currency_kurs_map = dict(
                zip(currency_map["short"], currency_map["Kurs"].apply(_to_float))
            )
    fx_by_verdepapper = {}
    sektor_by_verdepapper = {}
    if not taggar_df.empty and "Modellnamn" in taggar_df.columns:
        if "Kurs" in taggar_df.columns:
            kurs_map = (
                taggar_df[["Modellnamn", "Kurs"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            kurs_map = kurs_map[kurs_map["modell"] != ""]
            kurs_by_verdepapper = dict(
                zip(
                    kurs_map["modell"].str.casefold(),
                    kurs_map["Kurs"].apply(_to_float),
                )
            )
        if "FX" in taggar_df.columns:
            fx_map = (
                taggar_df[["Modellnamn", "FX"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            fx_map = fx_map[fx_map["modell"] != ""]
            fx_by_verdepapper = dict(
                zip(fx_map["modell"].str.casefold(), fx_map["FX"])
            )
        if "Sektor" in taggar_df.columns:
            sektor_map = (
                taggar_df[["Modellnamn", "Sektor"]]
                .dropna()
                .assign(modell=lambda d: d["Modellnamn"].astype(str).str.strip())
            )
            sektor_map = sektor_map[sektor_map["modell"] != ""]
            sektor_by_verdepapper = dict(
                zip(sektor_map["modell"].str.casefold(), sektor_map["Sektor"])
            )

    if not corev_actions.empty and "Värdepapper" in corev_actions.columns and "Antal" in corev_actions.columns:
        actions = corev_actions.copy()
        actions["Värdepapper"] = actions["Värdepapper"].astype(str).str.strip()
        actions["Värdepapper_norm"] = actions["Värdepapper"].str.casefold()
        actions["Antal"] = _to_float_series(actions["Antal"]).fillna(0)
        if "Datum" in actions.columns:
            actions["Datum"] = pd.to_datetime(actions["Datum"], errors="coerce")
            actions = actions.sort_values(by="Datum")
        if "Kurs" in actions.columns:
            actions["Kurs"] = _to_float_series(actions["Kurs"]).fillna(0)
        nettokassa = _latest_nettokassa(actions)
        for name, group in actions.groupby("Värdepapper_norm"):
            position = 0.0
            cost = 0.0
            for _, row in group.iterrows():
                qty = _to_float(row.get("Antal", 0)) or 0
                price = _to_float(row.get("Kurs", 0)) or 0
                if qty > 0:
                    cost += qty * price
                    position += qty
                elif qty < 0 and position > 0:
                    sell_qty = min(position, abs(qty))
                    avg_cost = cost / position if position else 0
                    cost -= avg_cost * sell_qty
                    position -= sell_qty
                if position <= 0:
                    position = 0.0
                    cost = 0.0
            gav_by_verdepapper[name] = (cost / position) if position else 0
        holdings = (
            actions.groupby("Värdepapper_norm", dropna=False)["Antal"]
            .sum()
            .reset_index()
        )
        name_map = (
            actions.dropna(subset=["Värdepapper_norm"])
            .groupby("Värdepapper_norm")["Värdepapper"]
            .first()
            .to_dict()
        )
        holdings = holdings[holdings["Värdepapper_norm"].astype(str).str.strip() != ""]
        holdings = holdings[holdings["Antal"] != 0]
        holdings["Värdepapper"] = holdings["Värdepapper_norm"].map(name_map).fillna(holdings["Värdepapper_norm"])
        holdings_rows = holdings.to_dict(orient="records")
        if nettokassa is not None:
            holdings_rows = [
                r
                for r in holdings_rows
                if str(r.get("Värdepapper", "")).strip().upper() not in {"KASSA", "SEK"}
            ]
            holdings_rows.append({"Värdepapper": "Kassa", "Antal": nettokassa})
        for row in holdings_rows:
            name = str(row.get("Värdepapper", "")).strip()
            name_norm = name.casefold()
            if name.upper() in {"KASSA", "SEK"}:
                row["Värdepapper"] = "Kassa"
                row["Kurs"] = 1
                row["FX"] = "SEK"
                row["Sektor"] = ""
                row["Värde"] = _to_float(row.get("Antal", 0)) or 0
            else:
                kurs = kurs_by_verdepapper.get(name_norm, "")
                row["Kurs"] = kurs
                fx_code = fx_by_verdepapper.get(name_norm, "")
                row["FX"] = fx_code
                row["Sektor"] = sektor_by_verdepapper.get(name_norm, "")
                fx_rate = _to_float(currency_kurs_map.get(str(fx_code).strip(), 1)) or 1
                if kurs not in ("", None):
                    value = (row.get("Antal", 0) or 0) * kurs * fx_rate
                else:
                    value = 0
                row["Värde"] = value
        total_value = sum((_to_float(r.get("Värde", 0)) or 0) for r in holdings_rows)
        for row in holdings_rows:
            val = _to_float(row.get("Värde", 0)) or 0
            row["Vikt"] = (val / total_value) if total_value else 0
            row["GAV"] = gav_by_verdepapper.get(str(row.get("Värdepapper", "")).strip().casefold(), 0)
            gav_val = _to_float(row.get("GAV", 0)) or 0
            kurs_val = _to_float(row.get("Kurs", 0)) or 0
            fx_code = str(row.get("FX", "")).strip()
            fx_rate = _to_float(currency_kurs_map.get(fx_code, 1)) or 1
            kurs_val_adj = (
                kurs_val * fx_rate
                if fx_code.upper() not in {"", "SEK"}
                else kurs_val
            )
            row["Utv"] = (kurs_val_adj / gav_val - 1) if gav_val else 0
        sek_rows = [r for r in holdings_rows if str(r.get("Värdepapper", "")).strip().upper() == "KASSA"]
        other_rows = [r for r in holdings_rows if str(r.get("Värdepapper", "")).strip().upper() != "KASSA"]
        other_rows = sorted(other_rows, key=lambda r: _to_float(r.get("Vikt", 0)) or 0, reverse=True)
        holdings_rows = other_rows + sek_rows
    sector_totals = {}
    utv_points = []
    if holdings_rows:
        for row in holdings_rows:
            if str(row.get("Värdepapper", "")).strip().upper() == "KASSA":
                continue
            sektor = str(row.get("Sektor", "")).strip()
            if not sektor:
                continue
            sector_totals[sektor] = sector_totals.get(sektor, 0) + (_to_float(row.get("Värde", 0)) or 0)
        for row in holdings_rows:
            name = str(row.get("Värdepapper", "")).strip()
            if name.upper() == "KASSA":
                continue
            utv_val = _to_float(row.get("Utv", 0)) or 0
            utv_points.append((name, utv_val))
        utv_points = sorted(utv_points, key=lambda x: x[1], reverse=True)
    if not corev_data.empty and "Datum" in corev_data.columns:
        corev_data = corev_data.copy()
        corev_data["__date"] = _parse_date_series(corev_data["Datum"])
        corev_data = corev_data.sort_values(by="__date", ascending=False).drop(columns=["__date"])
    if not corev_actions.empty:
        corev_actions = _ensure_row_id(corev_actions, "corevactions")
    if not corev_actions.empty and "Datum" in corev_actions.columns:
        corev_actions = corev_actions.copy()
        corev_actions["__date"] = _parse_date_series(corev_actions["Datum"])
        corev_actions = corev_actions.sort_values(by="__date", ascending=False).drop(columns=["__date"])
    data_cols = corev_data.columns.tolist() if not corev_data.empty else []
    action_cols = [c for c in corev_actions.columns.tolist() if c != "row_id"] if not corev_actions.empty else []
    data_rows = _safe_rows(corev_data) if not corev_data.empty else []
    action_rows = _safe_rows(corev_actions) if not corev_actions.empty else []
    return templates.TemplateResponse(
        "core_varlden.html",
        {
            "request": request,
            "data_cols": data_cols,
            "data_rows": data_rows,
            "action_cols": action_cols,
            "action_rows": action_rows,
            "holdings_rows": holdings_rows,
            "holdings_total_value": total_value if "total_value" in locals() else 0,
            "performance_rows": performance_rows,
            "performance_years": performance_years,
            "chart_points": json.dumps(chart_points),
            "sector_totals": json.dumps(sector_totals),
            "utv_points": json.dumps(utv_points),
            "format_cell": format_cell,
            "format_percent": format_percent,
            "format_percent_1": format_percent_1,
        },
    )


@app.get("/mandat/compliance-export")
def mandat_compliance_export(q: str = ""):
    rows, number_col = _prepare_mandat_rows_for_compliance(q)
    if not rows:
        export_df = pd.DataFrame(columns=["Number", "Kund", "Mandat", "Mandatnotering", "Rule", "Anledning"])
    else:
        compliance_rows = _build_compliance_rows(rows, number_col)
        export_df = pd.DataFrame(
            compliance_rows,
            columns=["Number", "Kund", "Mandat", "Mandatnotering", "Rule", "Innehav"],
        ).rename(columns={"Innehav": "Anledning"})
    output = BytesIO()
    export_df.to_excel(output, index=False, sheet_name="Compliance")
    output.seek(0)
    headers = {"Content-Disposition": "attachment; filename=compliance_report.xlsx"}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@app.get("/mandat/export")
def mandat_export(q: str = ""):
    df = _load_sheet("Mandat")
    if df.empty:
        export_df = pd.DataFrame()
    else:
        number_col = "Number" if "Number" in df.columns else "Nummer"
        if q and number_col in df.columns:
            df = df[df[number_col].astype(str).str.strip() == q.strip()]
        df = df.where(pd.notna(df), "")
        export_df = df.copy()
    output = BytesIO()
    export_df.to_excel(output, index=False, sheet_name="Mandat")
    output.seek(0)
    headers = {"Content-Disposition": "attachment; filename=mandat.xlsx"}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
