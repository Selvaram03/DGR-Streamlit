import pandas as pd

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
    "Vinathi_2": 2,
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
    "Vinathi_2": 25.00,
}


def clean_dataframe(df: pd.DataFrame, customer: str):
    """Clean dataframe and extract inverter/irradiation columns."""
    if df.empty:
        return df, [], None

    if customer == "TMD":
        inverter_cols = [c for c in df.columns if "GenPowerToday" in c]
    elif customer in ["BEL2", "BEL1"]:
        inverter_cols = [c for c in df.columns if "Meter_Generation" in c]
    elif customer == "PGCIL":
        inverter_cols = ["Total_Daily_Generation"]
    else:
        inverter_cols = [c for c in df.columns if "Daily_Generation" in c]

    for col in inverter_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    irradiation_col = next((c for c in df.columns if "Irradiation" in c), None)
    if irradiation_col:
        df[irradiation_col] = pd.to_numeric(df[irradiation_col], errors="coerce").fillna(0)

    df = df.sort_values("ts")
    return df, inverter_cols, irradiation_col


def calculate_kpis(customer, daily_generation, monthly_generation):
    """Calculate total daily, monthly generation and PLF based on customer-specific constants."""
    num_inverters = CUSTOMER_INVERTERS[customer]
    plf_base = PLF_BASE[customer]
    total_daily_gen = daily_generation.sum()
    total_monthly_gen = monthly_generation.sum()
    plf_percent = (total_daily_gen / (24 * plf_base * num_inverters)) if num_inverters else 0
    return total_daily_gen, total_monthly_gen, plf_percent
