
from __future__ import annotations

import os
import re
from datetime import datetime
from typing import List, Optional, Dict

import pdfplumber
import pandas as pd


# -----------------------------
# Helpers
# -----------------------------
def extract_statement_id(pdf_path: str) -> Optional[str]:
    """
    Extracts trailing numeric ID from filenames like:
    Донских_А_А_о_движении_денежных_средств_ozonbank_document_22873047.pdf
    -> 22873047
    """
    name = os.path.basename(pdf_path)
    m = re.search(r"_(\d+)\.pdf$", name, re.IGNORECASE)
    return m.group(1) if m else None


def norm_spaces(s: str) -> str:
    # Also normalize common PDF artifacts
    return re.sub(r"\s+", " ", (s or "").replace("\u00a0", " ").strip())


AMOUNT_CLEAN_RE = re.compile(r"[^0-9,\.\- ]")


def parse_rub_amount(x: str) -> Optional[float]:
    if not x:
        return None
    t = norm_spaces(x)
    t = t.replace("₽", "").replace("RUB", "").replace("руб.", "").strip()
    t = t.replace("- ", "-")  # "- 1 000,00" -> "-1000,00"
    t = AMOUNT_CLEAN_RE.sub("", t).replace(" ", "")

    if not t or t in {"-", ",", "."}:
        return None

    # decimal heuristics
    if "," in t and "." not in t:
        t = t.replace(",", ".")
    elif "," in t and "." in t:
        # last separator is decimal, remove the other as thousands sep
        if t.rfind(",") > t.rfind("."):
            t = t.replace(".", "").replace(",", ".")
        else:
            t = t.replace(",", "")

    try:
        return float(t)
    except ValueError:
        return None


# Matches:
# 12.01.2026 17:42:09
# 12.01.2026 17:42
# 12.01.2026
DATE_RE = re.compile(r"(\d{2}\.\d{2}\.\d{4})(?:\s+(\d{2}:\d{2})(?::(\d{2}))?)?")


def parse_ru_dt_from_first_cell(cell: str) -> Optional[datetime]:
    """
    Robust extraction from the first column:
    - Works even if the PDF cell has line breaks or extra text.
    - Supports seconds (HH:MM:SS).
    """
    s = norm_spaces(cell)
    m = DATE_RE.search(s)
    if not m:
        return None

    d = m.group(1)
    hhmm = m.group(2)
    ss = m.group(3)

    try:
        if hhmm and ss:
            return datetime.strptime(f"{d} {hhmm}:{ss}", "%d.%m.%Y %H:%M:%S")
        if hhmm:
            return datetime.strptime(f"{d} {hhmm}", "%d.%m.%Y %H:%M")
        return datetime.strptime(d, "%d.%m.%Y")
    except ValueError:
        return None


# -----------------------------
# Table post-processing
# -----------------------------

def is_probably_header_row(row: List[Optional[str]]) -> bool:
    joined = " ".join([norm_spaces(x or "") for x in row]).lower()
    markers = ["дата", "время", "документ", "назнач", "сумм", "операц"]
    return sum(m in joined for m in markers) >= 2


def clean_row(row: List[Optional[str]]) -> List[str]:
    return [norm_spaces(x or "") for x in row]


def merge_multiline_rows(rows: List[List[str]]) -> List[List[str]]:
    """
    If 1st column (date) is empty -> continuation of previous row.
    (Purpose often wraps into the next physical line.)
    """
    merged: List[List[str]] = []
    for r in rows:
        if not any(r):
            continue

        first = r[0] if len(r) > 0 else ""
        if first == "" and merged:
            prev = merged[-1]
            for i, cell in enumerate(r):
                if not cell:
                    continue
                if i < len(prev) and prev[i]:
                    prev[i] = norm_spaces(prev[i] + " " + cell)
                elif i < len(prev):
                    prev[i] = cell
                else:
                    prev.extend([""] * (i - len(prev) + 1))
                    prev[i] = cell
            merged[-1] = prev
        else:
            merged.append(r)

    return merged


# -----------------------------
# Extraction
# -----------------------------

def extract_transactions(pdf_path: str) -> pd.DataFrame:
    all_rows: List[List[str]] = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # Try default extraction first
            tables = page.extract_tables()

            # Fallback: line-based extraction
            if not tables:
                tables = page.extract_tables(
                    table_settings={
                        "vertical_strategy": "lines",
                        "horizontal_strategy": "lines",
                        "intersection_tolerance": 5,
                        "snap_tolerance": 3,
                        "join_tolerance": 3,
                        "edge_min_length": 20,
                        "min_words_vertical": 1,
                        "min_words_horizontal": 1,
                        "keep_blank_chars": False,
                    }
                )

            for tbl in tables or []:
                if not tbl:
                    continue
                cleaned = [clean_row(r) for r in tbl if r is not None]
                cleaned = [r for r in cleaned if not is_probably_header_row(r)]
                all_rows.extend(cleaned)

    all_rows = merge_multiline_rows(all_rows)
    all_rows = [r for r in all_rows if sum(1 for x in r if x) >= 2]

    if not all_rows:
        return pd.DataFrame(columns=["date", "document", "purpose", "amount_rub"])

    max_cols = max(len(r) for r in all_rows)
    padded = [r + [""] * (max_cols - len(r)) for r in all_rows]

    records: List[Dict[str, object]] = []

    for r in padded:
        # Date strictly in the first column, may include seconds
        dt = parse_ru_dt_from_first_cell(r[0])

        # Adjust if your statement has different order
        document = r[1] if max_cols > 1 else ""
        purpose = r[2] if max_cols > 2 else ""

        # Amount: try last column first; if empty, also try second-last (some layouts)
        amount = parse_rub_amount(r[-1]) or (parse_rub_amount(r[-2]) if max_cols >= 2 else None)

        # Skip noise lines
        if dt is None and amount is None:
            continue

        records.append(
            {
                "date": dt,
                "document": document,
                "purpose": purpose,
                "amount_rub": amount,
            }
        )

    df = pd.DataFrame(records)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        # df = df.sort_values("date", kind="stable").reset_index(drop=True)

    return df


def pdf_to_excel_transactions_only(pdf_path: str, out_xlsx: str, sheet_name: str = "Transactions") -> pd.DataFrame:
    df = extract_transactions(pdf_path)

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

        # Basic formatting
        ws = writer.book[sheet_name]
        ws.freeze_panes = "A2"
        for col in ws.columns:
            max_len = 0
            col_letter = col[0].column_letter
            for cell in col[:200]:
                if cell.value is None:
                    continue
                max_len = max(max_len, len(str(cell.value)))
            ws.column_dimensions[col_letter].width = min(max(10, max_len + 2), 70)

    return df


if __name__ == "__main__":
    pdf_path = "26450959.pdf"

    statement_id = extract_statement_id(pdf_path)

    if statement_id:
        out_xlsx = f"ozonbank_transactions_only_{statement_id}.xlsx"
    else:
        out_xlsx = "ozonbank_transactions_only.xlsx"

    pdf_to_excel_transactions_only(pdf_path, out_xlsx)
    print(f"Saved: {out_xlsx}")
