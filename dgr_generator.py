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

    df[inverter_cols] = df[inverter_cols].fillna(0)

    # Detect Irradiation column
    irradiation_col = None
    for col in df.columns:
        if "Irradiation" in col:
            irradiation_col = col
            df[col] = df[col].fillna(0)

    df["day"] = pd.to_datetime(df["day"]).dt.strftime("%Y-%m-%d")
    df = df.sort_values("day")

    return df, inverter_cols, irradiation_col


# ----------------------------
# Daily & Monthly Calculations
# ----------------------------
def get_daily_monthly_data(df, inverter_cols, month_start, report_date, irradiation_col=None, customer=None):
    report_date_prev = report_date - pd.Timedelta(days=1)
    prev_day_str = report_date_prev.strftime("%Y-%m-%d")

    # --- DAILY GENERATION ---
    if customer == "PGCIL":
        df["Total_Daily_Generation_kWh"] = df["Total_Daily_Generation"] * 1000
        last_10 = df.tail(10)
        daily_row = last_10.iloc[0] if not last_10.empty else None
        daily_generation_val = daily_row["Total_Daily_Generation_kWh"] if daily_row is not None else 0
        daily_generation = pd.Series([daily_generation_val], index=["Total_Daily_Generation_kWh"])
        inverter_names = ["Total_Meter_Generation"]

    elif customer in ["BEL2", "BEL1"]:
        meter_col = [c for c in inverter_cols if "Meter_Generation" in c][0]
        daily_row = df.loc[df["day"] == prev_day_str]
        daily_generation_val = daily_row[meter_col].iloc[0] if not daily_row.empty else 0
        daily_generation = pd.Series([daily_generation_val], index=[meter_col])
        inverter_names = ["Total_Meter_Generation"]

    else:
        daily_row = df.loc[df["day"] == prev_day_str]
        daily_generation = (
            daily_row[inverter_cols].iloc[0]
            if not daily_row.empty
            else pd.Series([0] * len(inverter_cols), index=inverter_cols)
        )
        inverter_names = [f"Inverter-{i+1}" for i in range(len(inverter_cols))]

    # --- DAILY IRRADIATION ---
    daily_irradiation = None
    monthly_avg_irradiation = None
    if irradiation_col:
        if customer == "PGCIL":
            last_10 = df[irradiation_col].tail(10)
            daily_irradiation = last_10.iloc[0] if not last_10.empty else 0
        else:
            daily_row_val = df.loc[df["day"] == prev_day_str]
            daily_irradiation = daily_row_val[irradiation_col].iloc[0] if not daily_row_val.empty else 0

        # Monthly average irradiation
        month_dates = pd.date_range(start=month_start, end=report_date)
        df["day_dt"] = pd.to_datetime(df["day"])
        merged_df = pd.DataFrame({"day_dt": month_dates}).merge(df, on="day_dt", how="left")
        merged_df[irradiation_col] = merged_df[irradiation_col].fillna(0)
        if customer == "PGCIL":
            last_10_monthly = merged_df[irradiation_col].tail(10)
            monthly_avg_irradiation = last_10_monthly.iloc[0] if not last_10_monthly.empty else 0
        else:
            monthly_avg_irradiation = merged_df[irradiation_col].mean()

    # --- MONTHLY GENERATION ---
    month_dates = pd.date_range(start=month_start, end=report_date)
    df["day_dt"] = pd.to_datetime(df["day"])
    merged_df = pd.DataFrame({"day_dt": month_dates}).merge(df, on="day_dt", how="left")

    if customer == "PGCIL":
        merged_df["Total_Daily_Generation_kWh"] = merged_df["Total_Daily_Generation"] * 1000
        monthly_generation = pd.Series(
            [merged_df["Total_Daily_Generation_kWh"].sum()],
            index=["Total_Daily_Generation_kWh"]
        )
    elif customer in ["BEL2", "BEL1"]:
        meter_col = [c for c in inverter_cols if "Meter_Generation" in c][0]
        merged_df[meter_col] = merged_df[meter_col].fillna(0)
        monthly_generation = pd.Series([merged_df[meter_col].sum()], index=[meter_col])
    else:
        merged_df[inverter_cols] = merged_df[inverter_cols].fillna(0)
        monthly_generation = merged_df[inverter_cols].sum()

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
    total_daily_gen = daily_generation.sum()
    total_monthly_gen = monthly_generation.sum()
    plf_percent = total_daily_gen / (24 * plf_base * num_inverters)
    return total_daily_gen, total_monthly_gen, plf_percent
