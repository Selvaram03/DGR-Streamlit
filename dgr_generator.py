# dgr_generator.py
import pandas as pd
from datetime import datetime

# ----------------------------
# Configuration Constants
# ----------------------------
CUSTOMER_INVERTERS = {
    "Imagica": 18,
    "BEL2": 1,
    "BEL1": 1,
    "Caspro": 11,
    "Dunung": 13,
    "Kasturi": 23,
    "Mauryaa": 13,
    "Paranjape": 19,
    "Vinathi_3": 25,
    "Vinathi_4": 15,
    "TMD": 9,
    "PGCIL": 32,
    "Vinathi_2": 2
}

PLF_BASE = {
    "Imagica": 3.06,
    "PGCIL": 26.56,
    "TMD": 10,
    "BEL2": 20,
    "Caspro": 3.05,
    "Dunung": 3.08,
    "Kasturi": 3.00,
    "Paranjape": 2.11,
    "Mauryaa": 3.08,
    "Vinathi_3": 3.00,
    "Vinathi_4": 3.07,
    "BEL1": 10.00,
    "Vinathi_2": 25.00
}

TMD_INVERTER_COLS = [
    "T1_CIS01_INV1_1_GenPowerToday",
    "T1_CIS01_INV1_2_GenPowerToday",
    "T1_CIS01_INV1_3_GenPowerToday",
    "T1_CIS01_INV1_4_GenPowerToday",
    "T1_CIS02_INV1_1_GenPowerToday",
    "T1_CIS02_INV1_2_GenPowerToday",
    "T1_CIS02_INV1_3_GenPowerToday",
    "T2_INV1_GenPowerToday",
    "T2_INV2_GenPowerToday"
]


def _detect_inverter_cols(df: pd.DataFrame, customer: str):
    """Return inverter column list (robust)"""
    if customer == "TMD":
        inverter_cols = [c for c in TMD_INVERTER_COLS if c in df.columns]
    elif customer in ["BEL2", "BEL1"]:
        inverter_cols = [c for c in df.columns if "Meter_Generation" in c]
    elif customer == "PGCIL":
        inverter_cols = ["Total_Daily_Generation"] if "Total_Daily_Generation" in df.columns else []
    else:
        inverter_cols = [
            c for c in df.columns if (
                c.startswith("Daily_Generation") or
                c.startswith("Daily_Generation_INV") or
                "GenPowerToday" in c or
                "Meter_Generation" in c
            )
        ]
    return inverter_cols


def clean_dataframe(df: pd.DataFrame, customer: str):
    """
    Light cleaning:
      - ensures inverter columns exist and are numeric
      - finds irradiation column (first matching)
      - keeps day field (if exists) as YYYY-MM-DD
    """
    df = df.copy()
    if df.empty:
        return df, [], None

    # ensure ts/day if present are handled safely
    if "day" in df.columns:
        try:
            df["day"] = pd.to_datetime(df["day"], errors="coerce").dt.strftime("%Y-%m-%d")
        except Exception:
            df["day"] = df["day"].astype(str)

    inverter_cols = _detect_inverter_cols(df, customer)
    # Guard: if none found, create placeholder so subsequent code doesn't break
    if not inverter_cols:
        placeholder = "gen_placeholder"
        if placeholder not in df.columns:
            df[placeholder] = 0
        inverter_cols = [placeholder]

    # Coerce inverter cols to numeric and fillna(0)
    for c in inverter_cols:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)

    # Irradiation detection (first column containing 'Irradiation')
    irradiation_col = None
    for c in df.columns:
        if "irradiation" in c.lower():
            irradiation_col = c
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            break

    # sort by ts if present for stability
    if "ts" in df.columns:
        try:
            df = df.sort_values("ts")
        except Exception:
            pass

    return df, inverter_cols, irradiation_col


# ----------------------------
# REPORT MODE: original behavior (yesterday)
# ----------------------------
def get_daily_monthly_data_report(df: pd.DataFrame, inverter_cols, month_start, report_date, irradiation_col=None, customer=None):
    """
    Report mode:
      - daily = previous day total (Option A chosen earlier)
      - monthly = sum over month range
    """
    # ensure day col exists
    if "day" not in df.columns:
        df["day"] = pd.to_datetime(df.get("ts"), errors="coerce").dt.strftime("%Y-%m-%d")

    report_date_prev = report_date - pd.Timedelta(days=1)
    prev_day_str = report_date_prev.strftime("%Y-%m-%d")

    # --- DAILY GENERATION (previous day)
    if customer == "PGCIL":
        # convert to kWh (original code multiplied by 1000)
        df["Total_Daily_Generation_kWh"] = pd.to_numeric(df.get("Total_Daily_Generation", 0), errors="coerce").fillna(0) * 1000
        day_rows = df.loc[df["day"] == prev_day_str].sort_values("ts", ascending=False)
        if not day_rows.empty:
            # pick 10th latest if available else most recent
            idx = 9 if len(day_rows) > 9 else 0
            daily_val = day_rows.iloc[idx]["Total_Daily_Generation_kWh"]
            daily_generation = pd.Series([daily_val], index=["Total_Daily_Generation_kWh"])
            inverter_names = ["Total_Meter_Generation"]
        else:
            daily_generation = pd.Series([0], index=["Total_Daily_Generation_kWh"])
            inverter_names = ["Total_Meter_Generation"]

    elif customer in ["BEL1", "BEL2"]:
        meter_cols = [c for c in inverter_cols if "Meter_Generation" in c]
        meter_col = meter_cols[0] if meter_cols else (inverter_cols[0] if inverter_cols else None)
        day_rows = df.loc[df["day"] == prev_day_str]
        if not day_rows.empty and meter_col is not None:
            daily_val = pd.to_numeric(day_rows.iloc[0].get(meter_col, 0), errors="coerce")
            daily_generation = pd.Series([daily_val], index=[meter_col])
            inverter_names = [meter_col]
        else:
            daily_generation = pd.Series([0], index=[meter_col or "Meter_Total"])
            inverter_names = [meter_col or "Meter_Total"]

    else:
        day_rows = df.loc[df["day"] == prev_day_str]
        if not day_rows.empty:
            # pick the row with latest ts for that day
            row = day_rows.sort_values("ts", ascending=False).iloc[0]
            daily_generation = row[inverter_cols].astype(float)
            inverter_names = [f"Inverter-{i+1}" for i in range(len(inverter_cols))]
        else:
            daily_generation = pd.Series([0] * len(inverter_cols), index=inverter_cols)
            inverter_names = [f"Inverter-{i+1}" for i in range(len(inverter_cols))]

    # --- DAILY IRRADIATION and monthly average irradiation
    daily_irradiation = None
    monthly_avg_irradiation = None
    if irradiation_col:
        if customer == "PGCIL":
            vals = df[irradiation_col].dropna()
            if not vals.empty:
                last10 = df.sort_values("ts", ascending=False).head(10)
                daily_irradiation = float(last10.iloc[-1].get(irradiation_col, 0)) if not last10.empty else 0.0
            else:
                daily_irradiation = 0.0
        else:
            day_row_val = df.loc[df["day"] == prev_day_str]
            daily_irradiation = float(day_row_val.iloc[0].get(irradiation_col, 0)) if not day_row_val.empty else 0.0

        # monthly average
        month_dates = pd.date_range(start=month_start, end=report_date)
        df["day_dt"] = pd.to_datetime(df["day"], errors="coerce")
        merged = pd.DataFrame({"day_dt": month_dates}).merge(df, on="day_dt", how="left")
        merged[irradiation_col] = pd.to_numeric(merged.get(irradiation_col, 0), errors="coerce").fillna(0)
        monthly_avg_irradiation = float(merged[irradiation_col].mean())

    # --- MONTHLY GENERATION
    month_dates = pd.date_range(start=month_start, end=report_date)
    df["day_dt"] = pd.to_datetime(df["day"], errors="coerce")
    merged = pd.DataFrame({"day_dt": month_dates}).merge(df, on="day_dt", how="left")

    if customer == "PGCIL":
        merged["Total_Daily_Generation_kWh"] = pd.to_numeric(merged.get("Total_Daily_Generation", 0), errors="coerce").fillna(0) * 1000
        monthly_generation = pd.Series([merged["Total_Daily_Generation_kWh"].sum()], index=["Total_Daily_Generation_kWh"])
    elif customer in ["BEL1", "BEL2"]:
        meter_col = [c for c in inverter_cols if "Meter_Generation" in c]
        meter_col = meter_col[0] if meter_col else (inverter_cols[0] if inverter_cols else None)
        if meter_col:
            merged[meter_col] = pd.to_numeric(merged.get(meter_col, 0), errors="coerce").fillna(0)
            monthly_generation = pd.Series([merged[meter_col].sum()], index=[meter_col])
        else:
            merged[inverter_cols] = merged[inverter_cols].fillna(0)
            monthly_generation = merged[inverter_cols].sum()
    else:
        merged[inverter_cols] = merged[inverter_cols].fillna(0)
        monthly_generation = merged[inverter_cols].sum()

    # final DF for report visualization
    daily_idx = list(daily_generation.index)
    monthly_idx = list(monthly_generation.index) if hasattr(monthly_generation, "index") else daily_idx

    # align indices
    if daily_idx == monthly_idx:
        inv_index = daily_idx
    else:
        # fallback: create generic inverter names
        inv_index = [f"Inverter-{i+1}" for i in range(max(len(daily_idx), len(monthly_idx)))]

    daily_vals = [daily_generation.get(i, 0) for i in inv_index]
    monthly_vals = [monthly_generation.get(i, 0) for i in inv_index]

    final_df = pd.DataFrame({
        "Inverter": inv_index,
        "Daily Generation (kWh)": daily_vals,
        "Monthly Generation (kWh)": monthly_vals
    })

    return final_df, daily_generation, monthly_generation, daily_irradiation, monthly_avg_irradiation


# ----------------------------
# LIVE MODE: latest row only (Option A)
# ----------------------------
def get_daily_monthly_data_live(df: pd.DataFrame, inverter_cols, irradiation_col=None, customer=None):
    """
    df: expected to be single-row DataFrame (latest row)
    Returns tuple similar to report function:
      final_df, daily_generation (Series), monthly_generation (Series), daily_irr, monthly_avg_irr
    For LIVE we return daily==sum of inverter cols in latest row (Option A).
    monthly_generation returned as same as daily to keep KPI logic working.
    """
    if df.empty:
        # empty return
        daily_series = pd.Series([0] * len(inverter_cols), index=inverter_cols)
        monthly_series = daily_series.copy()
        final_df = pd.DataFrame({
            "Inverter": [f"Inverter-{i+1}" for i in range(len(inverter_cols))],
            "Daily Generation (kWh)": daily_series.values,
            "Monthly Generation (kWh)": monthly_series.values
        })
        return final_df, daily_series, monthly_series, 0.0, 0.0

    # take the latest row (should be only row)
    row = df.iloc[-1]
    # For PGCIL and BEL use meter logic if present
    if customer == "PGCIL":
        if "Total_Daily_Generation" in df.columns:
            val = pd.to_numeric(row.get("Total_Daily_Generation", 0), errors="coerce")
            daily_series = pd.Series([val * 1000], index=["Total_Daily_Generation_kWh"])
        else:
            daily_series = pd.Series([0], index=["Total_Daily_Generation_kWh"])
    elif customer in ["BEL1", "BEL2"]:
        meter_cols = [c for c in inverter_cols if "Meter_Generation" in c]
        meter_col = meter_cols[0] if meter_cols else (inverter_cols[0] if inverter_cols else None)
        if meter_col:
            val = pd.to_numeric(row.get(meter_col, 0), errors="coerce")
            daily_series = pd.Series([val], index=[meter_col])
        else:
            # fallback sum of inverter cols
            vals = [pd.to_numeric(row.get(c, 0), errors="coerce") for c in inverter_cols]
            daily_series = pd.Series(vals, index=inverter_cols)
    else:
        # generic: take each inverter value; also provide total as sum if desired
        vals = [pd.to_numeric(row.get(c, 0), errors="coerce") for c in inverter_cols]
        daily_series = pd.Series(vals, index=inverter_cols)

    # monthly_series mirror daily_series (so KPI function works)
    monthly_series = daily_series.copy()

    # daily irradiation
    daily_irr = float(pd.to_numeric(row.get(irradiation_col, 0), errors="coerce")) if irradiation_col else 0.0
    monthly_irr = daily_irr  # not meaningful, but keep numeric

    # final df: per-inverter rows
    final_df = pd.DataFrame({
        "Inverter": list(daily_series.index),
        "Daily Generation (kWh)": list(daily_series.values),
        "Monthly Generation (kWh)": list(monthly_series.values)
    })

    return final_df, daily_series, monthly_series, daily_irr, monthly_irr


# ----------------------------
# KPI Calculation
# ----------------------------
def calculate_kpis(customer, daily_generation: pd.Series, monthly_generation: pd.Series):
    """Return numeric scalars: total_daily, total_monthly, plf_percent"""
    num_inverters = float(CUSTOMER_INVERTERS.get(customer, 1))
    plf_base = float(PLF_BASE.get(customer, 1.0))

    total_daily_gen = float(daily_generation.sum()) if hasattr(daily_generation, "sum") else float(daily_generation)
    total_monthly_gen = float(monthly_generation.sum()) if hasattr(monthly_generation, "sum") else float(monthly_generation)

    denom = 24 * plf_base * num_inverters
    plf_percent = (total_daily_gen / denom) if denom != 0 else 0.0

    return total_daily_gen, total_monthly_gen, plf_percent
