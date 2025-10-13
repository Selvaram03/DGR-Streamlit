# dgr_generator.py
import pandas as pd

# --- Customer inverter mapping for PLF ---
CUSTOMER_INVERTERS = {
    "Imagica": 18,
    "BEL2": 1,
    "Rajgir": 1,
    "Caspro": 11,
    "Dunung": 13,
    "Kasturi": 23,
    "Mauryaa": 13,
    "Paranjape": 19,
    "Vinathi_3": 25,
    "Vinathi_4": 15,
    "TMD": 9,
    "PGCIL": 32
}

def clean_dataframe(df: pd.DataFrame):
    """Fill missing inverter generation columns with 0."""
    inverter_cols = [c for c in df.columns if any([
        c.startswith("Daily_Generation_INV"),
        c.startswith("T1_"),
        c.startswith("T2_"),
        "Meter_Generation" in c,
        "Irradiation" in c
    ])]
    df[inverter_cols] = df[inverter_cols].fillna(0)
    df["day"] = pd.to_datetime(df["day"]).dt.strftime("%Y-%m-%d")
    df = df.sort_values("day")
    return df, inverter_cols


def get_daily_monthly_data(df, inverter_cols, month_start, report_date):
    """Compute daily and monthly generation DataFrames."""
    report_date_prev = report_date - pd.Timedelta(days=1)
    prev_day_str = report_date_prev.strftime("%Y-%m-%d")

    daily_row = df.loc[df["day"] == prev_day_str]
    daily_generation = (
        daily_row[inverter_cols].iloc[0]
        if not daily_row.empty
        else pd.Series([0] * len(inverter_cols), index=inverter_cols)
    )

    month_dates = pd.date_range(start=month_start, end=report_date)
    df["day_dt"] = pd.to_datetime(df["day"])
    merged_df = pd.DataFrame({"day_dt": month_dates}).merge(df, on="day_dt", how="left")
    merged_df[inverter_cols] = merged_df[inverter_cols].fillna(0)
    monthly_generation = merged_df[inverter_cols].sum()

    daily_df = pd.DataFrame({
        "Inverter": [f"Inverter-{i+1}" for i in range(len(inverter_cols))],
        "Daily Generation (kWh)": daily_generation.values
    })

    monthly_df = pd.DataFrame({
        "Inverter": [f"Inverter-{i+1}" for i in range(len(inverter_cols))],
        "Monthly Generation (kWh)": monthly_generation.values
    })

    final_df = daily_df.merge(monthly_df, on="Inverter")
    return final_df, daily_generation, monthly_generation


def calculate_kpis(customer, daily_generation, monthly_generation):
    """Calculate PLF and total generation metrics."""
    num_inverters = CUSTOMER_INVERTERS.get(customer, len(daily_generation))
    total_daily_gen = daily_generation.sum()
    total_monthly_gen = monthly_generation.sum()
    plf_percent = (total_daily_gen / (24 * 3.04 * num_inverters))
    return total_daily_gen, total_monthly_gen, plf_percent

