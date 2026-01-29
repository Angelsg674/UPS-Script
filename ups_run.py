import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

from ups_utils import (
    find_col,
    safe_name,
    to_date,
    bucket,
    write_bucket_csv,
    not_blank,
    classify_idf_mdf,
    is_noc_contact,
    format_counts,
    list_overdue_locations,
)


def main():
    ap = argparse.ArgumentParser(
        description="Split Smartsheet UPS export by Contact into Battery/Unit buckets (CSV)."
    )
    ap.add_argument("input", help="Path to Smartsheet export (.xlsx or .csv)")
    ap.add_argument("--outdir", default="out", help="Output directory root (default: out)")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise SystemExit(f"Input file not found: {in_path}")

    # Load
    if in_path.suffix.lower() in [".xlsx", ".xls"]:
        df = pd.read_excel(in_path)
    elif in_path.suffix.lower() == ".csv":
        df = pd.read_csv(in_path)
    else:
        raise SystemExit("Unsupported input. Use .xlsx/.xls or .csv")

    # Columns
    contact_col = find_col(df, ["Contact"])
    loc_col = find_col(df, ["UPS Location and Hostname", "UPS Location & Hostname", "Location and Hostname"])
    ip_col = find_col(df, ["IP Address", "IP"])
    mac_col = find_col(df, ["MAC Address", "MAC"])
    battery_type_col = find_col(df, ["Battery Type", "Battery Model"])
    battery_due_col = find_col(df, ["Next Battery Replacement Date", "Next Battery Replacement"])
    unit_model_col = find_col(df, ["Unit Model"])
    unit_serial_col = find_col(df, ["Unit Serial #", "Unit Serial", "Serial #", "Serial"])
    unit_due_col = find_col(df, ["Unit replacement Date", "Unit Replacement Date", "Replacement Date"])

    missing = []
    for name, col in [
        ("Contact", contact_col),
        ("UPS Location and Hostname", loc_col),
        ("Battery Type", battery_type_col),
        ("Next Battery Replacement Date", battery_due_col),
        ("Unit replacement Date", unit_due_col),
    ]:
        if not col:
            missing.append(name)

    if missing:
        cols = "\n- ".join(map(str, df.columns))
        raise SystemExit(
            "Missing required columns in export: "
            + ", ".join(missing)
            + "\n\nColumns found:\n- "
            + cols
        )

    today = pd.Timestamp.today().normalize()

    # Prep values
    df[contact_col] = df[contact_col].fillna("UNASSIGNED")

    identifier_masks = []
    for c in [ip_col, mac_col, unit_serial_col, unit_model_col, battery_type_col]:
        if c:
            identifier_masks.append(not_blank(df[c]))

    if identifier_masks:
        has_identifier = identifier_masks[0]
        for m in identifier_masks[1:]:
            has_identifier = has_identifier | m
    else:
        has_identifier = pd.Series([True] * len(df), index=df.index)

    before = len(df)
    df = df[has_identifier].copy()
    after = len(df)
    print(f"Filtered out {before - after} non-unit header rows")

    # Closet classification
    df["closet_type"] = df[loc_col].apply(classify_idf_mdf)

    # Parse dates
    df[battery_due_col] = to_date(df[battery_due_col])
    df[unit_due_col] = to_date(df[unit_due_col])

    # Days until due
    df["battery_days"] = (df[battery_due_col] - today).dt.days
    df["unit_days"] = (df[unit_due_col] - today).dt.days

    # Buckets
    df["battery_bucket"] = df["battery_days"].apply(bucket)
    df["unit_bucket"] = df["unit_days"].apply(bucket)

    logical_order = [
        "0_3_MONTHS",
        "3_6_MONTHS",
        "6_12_MONTHS",
        "12_PLUS_MONTHS",
        "OVERDUE",
    ]
    # Convert to categorical so sorting respects the list above
    df["battery_bucket"] = pd.Categorical(df["battery_bucket"], categories=logical_order, ordered=True)
    df["unit_bucket"] = pd.Categorical(df["unit_bucket"], categories=logical_order, ordered=True)

    # ---------------
    # IMPORTANT RULES
    # ---------------
    # Rule 1: If battery due and unit due are within 365 days of each other,
    #         prefer replacing the unit.
    both_dates_present = df[battery_due_col].notna() & df[unit_due_col].notna()
    within_year = (df[unit_due_col] - df[battery_due_col]).abs().dt.days <= 365
    prefer_unit_due_to_proximity = both_dates_present & within_year

    # Rule 2: If BOTH are overdue, only log unit
    both_overdue = (df["battery_days"] < 0) & (df["unit_days"] < 0)

    # Final: suppress battery logging if either rule applies
    df["suppress_battery"] = prefer_unit_due_to_proximity | both_overdue

    # Mark NOC rows BEFORE creating df_battery_effective
    df["is_noc"] = df[contact_col].apply(is_noc_contact)

    # Battery dataset after suppression rules
    df_battery_effective = df[~df["suppress_battery"]].copy()
    df_battery_effective = df_battery_effective[df_battery_effective[battery_due_col].notna()].copy()

    # Output base
    stamp = datetime.now().strftime("%m-%d-%Y")
    out_root = Path(args.outdir) / stamp
    out_root.mkdir(parents=True, exist_ok=True)

    # Minimal output columns
    battery_out_cols = [contact_col, loc_col, battery_type_col, battery_due_col]
    unit_out_cols = [contact_col, loc_col, unit_due_col]
    if unit_model_col:
        unit_out_cols.insert(2, unit_model_col)
    if unit_serial_col:
        unit_out_cols.insert(3, unit_serial_col)

    # We iterate over the logical order so files are generated in a consistent loop
    bucket_list = logical_order

    # Split by Contact
    for contact, g_all in df.groupby(contact_col, dropna=False):
        contact_folder = out_root / "by_contact" / safe_name(contact)

        # Batteries (RESPECT suppression rules)
        g_bat = df_battery_effective[df_battery_effective[contact_col] == contact]
        bat_folder = contact_folder / "batteries"
        for b in bucket_list:
            subset = g_bat[g_bat["battery_bucket"] == b].copy()
            if len(subset) > 0:
                write_bucket_csv(subset, bat_folder / f"{b.lower()}.csv", battery_out_cols)

        # Units (always written)
        unit_folder = contact_folder / "units"
        for b in bucket_list:
            subset = g_all[g_all["unit_bucket"] == b].copy()
            if len(subset) > 0:
                write_bucket_csv(subset, unit_folder / f"{b.lower()}.csv", unit_out_cols)

    # -------------------------
    # Summary: NOC-focused MDF split OVERDUE location lists ONLY
    # -------------------------
    noc_all = df[df["is_noc"]].copy()
    noc_bat = df_battery_effective[df_battery_effective["is_noc"]].copy()
    other_df = df[~df["is_noc"]].copy()

    summary_lines = []
    summary_lines.append(f"Run date: {today.date()}")
    summary_lines.append(f"Input: {in_path}")
    summary_lines.append("")

    # Overall counts
    summary_lines.append("=== OVERALL COUNTS (ALL CONTACTS) ===")
    
    # sort=False ensures it respects the 0-3, 3-6, ... logical order
    summary_lines.append(
        format_counts(
            "Battery buckets",
            df_battery_effective["battery_bucket"].value_counts(sort=False, dropna=True),
        )
    )
    summary_lines.append("")
    summary_lines.append(format_counts("Unit buckets", df["unit_bucket"].value_counts(sort=False, dropna=True)))
    summary_lines.append("")
    summary_lines.append(f"Battery rows suppressed: {int(df['suppress_battery'].sum())}")
    summary_lines.append("")

    # NOC section
    summary_lines.append("=== NOC ONLY ===")
    if len(noc_all) == 0:
        summary_lines.append("No rows detected as NOC (Contact did not contain 'noc').")
        summary_lines.append("If your NOC contact string is different, adjust is_noc_contact().")
        summary_lines.append("")
    else:
        summary_lines.append("NOC Battery buckets by MDF/IDF:")
        # Pivot table 
        pivot_bat = pd.crosstab(noc_bat["closet_type"], noc_bat["battery_bucket"])
        summary_lines.append(pivot_bat.to_string())
        summary_lines.append("")

        summary_lines.append("NOC Unit buckets by MDF/IDF:")
        pivot_unit = pd.crosstab(noc_all["closet_type"], noc_all["unit_bucket"])
        summary_lines.append(pivot_unit.to_string())
        summary_lines.append("")

        # Overdue counts
        noc_bat_overdue = noc_bat[noc_bat["battery_bucket"] == "OVERDUE"]
        noc_unit_overdue = noc_all[noc_all["unit_bucket"] == "OVERDUE"]
        summary_lines.append(f"NOC Batteries OVERDUE: {len(noc_bat_overdue)}")
        summary_lines.append(f"NOC Units OVERDUE: {len(noc_unit_overdue)}")
        summary_lines.append("")

        # OVERDUE location lists ONLY
        list_overdue_locations(summary_lines, noc_bat_overdue, loc_col, "NOC Battery OVERDUE locations")
        summary_lines.append("")
        list_overdue_locations(summary_lines, noc_unit_overdue, loc_col, "NOC Unit OVERDUE locations")
        summary_lines.append("")

    # Other contacts
    summary_lines.append("=== OTHER CONTACTS ===")
    if len(other_df) == 0:
        summary_lines.append("None (all rows are NOC or UNASSIGNED).")
    else:
        actionable_other = other_df[
            other_df["battery_bucket"].isin(["OVERDUE", "0_3_MONTHS", "3_6_MONTHS", "6_12_MONTHS"])
            | other_df["unit_bucket"].isin(["OVERDUE", "0_3_MONTHS", "3_6_MONTHS", "6_12_MONTHS"])
        ]
        if len(actionable_other) == 0:
            summary_lines.append("No actionable items for non-NOC contacts (everything is 12+ months out).")
        else:
            counts = actionable_other[contact_col].value_counts(dropna=True)
            summary_lines.append("Actionable rows by Contact (within 12 months or overdue):")
            summary_lines.append(counts.to_string())

    (out_root / "summary.txt").write_text("\n".join(summary_lines), encoding="utf-8")

    print("\nDone ✅")
    print(f"Output folder:\n{out_root.resolve()}")
    print("\nExample path:")
    print(out_root / "by_contact" / "UNASSIGNED" / "batteries" / "overdue.csv")


if __name__ == "__main__":
    main()