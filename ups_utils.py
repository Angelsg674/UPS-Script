# ups_utils.py
from pathlib import Path
import re
import pandas as pd


def normalize(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    norm_map = {normalize(c): c for c in df.columns}
    for cand in candidates:
        key = normalize(cand)
        if key in norm_map:
            return norm_map[key]
    return None


def safe_name(x) -> str:
    s = str(x).strip()
    if not s or s.lower() == "nan":
        return "UNASSIGNED"
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_\-]+", "", s)
    return s or "UNASSIGNED"


def to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def bucket(days) -> str:
    if pd.isna(days):
        return "NO_DATE"
    if days < 0:
        return "OVERDUE"
    if days <= 90:
        return "0_3_MONTHS"
    if days <= 180:
        return "3_6_MONTHS"
    if days <= 365:
        return "6_12_MONTHS"
    return "12_PLUS_MONTHS"


def write_bucket_csv(group_df: pd.DataFrame, out_path: Path, cols: list[str]):
    cols = [c for c in cols if c in group_df.columns]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    group_df.loc[:, cols].to_csv(out_path, index=False)


def not_blank(series: pd.Series) -> pd.Series:
    return series.notna() & (series.astype(str).str.strip() != "")


def classify_idf_mdf(location_value) -> str:
    s = str(location_value).upper()
    if "MDF" in s:
        return "MDF"
    if "IDF" in s:
        return "IDF"
    return "UNKNOWN"


def is_noc_contact(contact_value) -> bool:
    s = str(contact_value).strip().lower()
    return "noc" in s


def format_counts(title: str, series: pd.Series) -> str:
    if series is None or len(series) == 0:
        return f"{title}:\nNone"
    return f"{title}:\n{series.to_string()}"


def list_overdue_locations(summary_lines: list[str], sub: pd.DataFrame, loc_col: str, title: str):
    if len(sub) == 0:
        summary_lines.append(f"{title}: None")
        return
    summary_lines.append(f"{title}:")
    tmp = sub[["closet_type", loc_col]].dropna().sort_values(["closet_type", loc_col])
    for _, r in tmp.iterrows():
        summary_lines.append(f"- {r['closet_type']}: {r[loc_col]}")
