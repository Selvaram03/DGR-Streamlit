import pandas as pd

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


# ----------------------------
# Data Cleaning
# ----------------------------
def clean_dataframe(df: pd.DataFrame, customer: str):
    """Clean and prepare data for generation and irradiation analysis."""
    if customer == "TMD":
        inverter_cols = [c for c in TMD_INVERTER_COLS if c in df.columns]
    elif customer in ["BEL2", "BEL1"]:
        inverter_cols = [c for c in df.columns if "Meter_Generation" in c]
    elif customer == "PGCIL":
        inverter_cols = ["Total_Daily_Generation"]
    else:
        inverter_cols = [
            c for c in df.columns if any([
                c.startswith("Daily_Generation"),
                c.startswith("Daily_Generation_INV"),
                c.startswith("T1_CIS"),
                c.startswith("T2_INV"),
                "Meter_Generation" in c
            ])
        ]

    if inverter_cols:
        df[inverter_cols] = df[inverter_cols].fillna(0)

    # Detect Irradiation column
    irradiation_col = None
    for col in df.columns:
        if "Irradiation" in col:
            irradiation_col = col
            df[col] = df[col].fillna(0)

    # Normalize & sort day
    df["day"] = pd.to_datetime(df["day"]).dt.strftime("%Y-%m-%d")
    df = df.sort_values("day")

    return df, inverter_cols, irradiation_col


# ----------------------------
# Daily & Monthly Calculations
# ----------------------------
def get_daily_monthly_data(
    df,
    inverter_cols,
    month_start,              # kept for backwards compatibility (ignored)
    report_date,
    irradiation_col=None,
    customer=None
):
    """
    Compute daily and month-to-date (MTD) values.

    IMPORTANT:
    - All calculations are aligned to the 'data day' = report_date - 1 day.
    - MTD window = [first_of_month(data_day), data_day]
    """
    # The day we actually have complete data for
    data_day = pd.to_datetime(report_date) - pd.Timedelta(days=1)
    data_day_str = data_day.strftime("%Y-%m-%d")

    # Derive MTD window from the data day
    month_start_local = pd.Timestamp(year=data_day.year, month=data_day.month, day=1)
    month_end_local = data_day

    # --- DAILY GENERATION ---
    if customer == "PGCIL":
        df["Total_Daily_Generation_kWh"] = df["Total_Daily_Generation"].fillna(0) * 1000
        daily_row = df.loc[df["day"] == data_day_str]
        daily_generation_val = (
            daily_row["Total_Daily_Generation_kWh"].iloc[0] if not daily_row.empty else 0
        )
        daily_generation = pd.Series([daily_generation_val], index=["Total_Daily_Generation_kWh"])
        inverter_names = ["Total_Meter_Generation"]

    elif customer in ["BEL2", "BEL1"]:
        meter_col = [c for c in inverter_cols if "Meter_Generation" in c][0]
        daily_row = df.loc[df["day"] == data_day_str]
        daily_generation_val = daily_row[meter_col].iloc[0] if not daily_row.empty else 0
        daily_generation = pd.Series([daily_generation_val], index=[meter_col])
        inverter_names = ["Total_Meter_Generation"]

    else:
        daily_row = df.loc[df["day"] == data_day_str]
        daily_generation = (
            daily_row[inverter_cols].iloc[0]
            if not daily_row.empty
            else pd.Series([0] * len(inverter_cols), index=inverter_cols)
        )
        inverter_names = [f"Inverter-{i+1}" for i in range(len(inverter_cols))]

    # --- STRICT MTD FILTER (prevents YTD bleed) ---
    df["day_dt"] = pd.to_datetime(df["day"])
    df_mtd = df[(df["day_dt"] >= month_start_local) & (df["day_dt"] <= month_end_local)].copy()

    # Build a dense calendar just for the MTD window, then left-join the filtered data
    month_dates = pd.date_range(start=month_start_local, end=month_end_local)
    base_calendar = pd.DataFrame({"day_dt": month_dates})
    merged_mtd = base_calendar.merge(df_mtd, on="day_dt", how="left")

    # --- DAILY / MONTHLY IRRADIATION ---
    daily_irradiation = None
    monthly_avg_irradiation = None
    if irradiation_col:
        # daily (from df)
        daily_row_val = df.loc[df["day"] == data_day_str]
        daily_irradiation = daily_row_val[irradiation_col].iloc[0] if not daily_row_val.empty else 0

        # monthly average (only within MTD window)
        merged_mtd[irradiation_col] = merged_mtd[irradiation_col].fillna(0)
        monthly_avg_irradiation = merged_mtd[irradiation_col].mean()

    # --- MONTHLY GENERATION (MTD only) ---
    if customer == "PGCIL":
        merged_mtd["Total_Daily_Generation_kWh"] = merged_mtd["Total_Daily_Generation"].fillna(0) * 1000
        monthly_generation = pd.Series(
            [merged_mtd["Total_Daily_Generation_kWh"].sum()],
            index=["Total_Daily_Generation_kWh"]
        )
    elif customer in ["BEL2", "BEL1"]:
        meter_col = [c for c in inverter_cols if "Meter_Generation" in c][0]
        merged_mtd[meter_col] = merged_mtd[meter_col].fillna(0)
        monthly_generation = pd.Series([merged_mtd[meter_col].sum()], index=[meter_col])
    else:
        if inverter_cols:
            merged_mtd[inverter_cols] = merged_mtd[inverter_cols].fillna(0)
            monthly_generation = merged_mtd[inverter_cols].sum()
        else:
            monthly_generation = pd.Series([], dtype=float)

    # --- Final DF for visualization ---
    daily_df = pd.DataFrame({
        "Inverter": inverter_names,
        "Daily Generation (kWh)": daily_generation.values
    })
    monthly_df = pd.DataFrame({
        "Inverter": inverter_names,
        "Monthly Generation (kWh)": monthly_generation.values
    })
    final_df = daily_df.merge(monthly_df, on="Inverter")

    return final_df, daily_generation, monthly_generation, daily_irradiation, monthly_avg_irradiation


# ----------------------------
# KPI Calculation
# ----------------------------
def calculate_kpis(customer, daily_generation, monthly_generation):
    """Calculate total daily, monthly generation and PLF based on customer-specific constants."""
    num_inverters = CUSTOMER_INVERTERS[customer]
    plf_base = PLF_BASE[customer]
    total_daily_gen = float(daily_generation.sum())
    total_monthly_gen = float(monthly_generation.sum())
    # Avoid division by zero
    denom = 24 * plf_base * num_inverters if (plf_base and num_inverters) else 1
    plf_percent = (total_daily_gen / denom)
    return total_daily_gen, total_monthly_gen, plf_percent


