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


# -------------------------
# NEW BUCKETING (requested)
# -------------------------
FISCAL_YEAR_START_MONTH = 7  # July 1 fiscal year start


def fiscal_year_end(today: pd.Timestamp) -> pd.Timestamp:
    """
    Fiscal year starts July 1.
    If today is on/after July 1, fiscal year ends June 30 next year.
    If today is before July 1, fiscal year ends June 30 this year.
    """
    today = pd.Timestamp(today).normalize()
    y = today.year
    end_year = (y + 1) if today.month >= FISCAL_YEAR_START_MONTH else y
    return pd.Timestamp(end_year, 6, 30)


def calendar_year_end(today: pd.Timestamp) -> pd.Timestamp:
    today = pd.Timestamp(today).normalize()
    return pd.Timestamp(today.year, 12, 31)


def bucket_due(due_date, today: pd.Timestamp) -> str:
    """
    Returns:
      - OVERDUE
      - BY_FISCAL_YEAR_END
      - BY_CALENDAR_YEAR_END
      - BEYOND_YEAR_ENDS
      - NO_DATE
    Uses the day the script is run (today).
    """
    if pd.isna(due_date):
        return "NO_DATE"

    due = pd.Timestamp(due_date).normalize()
    today = pd.Timestamp(today).normalize()

    if due < today:
        return "OVERDUE"

    fy_end = fiscal_year_end(today)
    cal_end = calendar_year_end(today)

    # If due date qualifies for BOTH, choose the earlier deadline bucket
    if due <= min(fy_end, cal_end):
        return "BY_CALENDAR_YEAR_END" if cal_end <= fy_end else "BY_FISCAL_YEAR_END"

    if due <= fy_end:
        return "BY_FISCAL_YEAR_END"
    if due <= cal_end:
        return "BY_CALENDAR_YEAR_END"

    return "BEYOND_YEAR_ENDS"


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
