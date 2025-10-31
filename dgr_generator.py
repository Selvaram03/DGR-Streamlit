# dgr_generator.py
import pandas as pd

# ----------------------------
# Config
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
# Cleaning
# ----------------------------
def clean_dataframe(df, customer):
    if df.empty:
        return df, [], None

    if customer == "TMD":
        inverter_cols = [c for c in TMD_INVERTER_COLS if c in df.columns]
    elif customer in ["BEL1", "BEL2"]:
        inverter_cols = [c for c in df.columns if "Meter_Generation" in c]
    elif customer == "PGCIL":
        inverter_cols = ["Total_Daily_Generation"]
    else:
        inverter_cols = [
            c for c in df.columns if (
                c.startswith("Daily_Generation") or
                "GenPowerToday" in c or
                "Meter_Generation" in c
            )
        ]

    df[inverter_cols] = df[inverter_cols].fillna(0)

    irradiation_col = None
    for c in df.columns:
        if "Irradiation" in c:
            irradiation_col = c
            df[c] = df[c].fillna(0)

    return df, inverter_cols, irradiation_col



# ----------------------------
# Daily & Monthly
# ----------------------------
def get_daily_monthly_data(df, inverter_cols, month_start, report_date, irradiation_col=None, customer=None):

    # Daily Generation
    if df.empty:
        daily = pd.Series([0] * len(inverter_cols), index=inverter_cols)
    else:
        row = df.tail(1).iloc[0]
        daily = row[inverter_cols]

    # Monthly (just sum all)
    monthly = df[inverter_cols].sum() if not df.empty else pd.Series([0] * len(inverter_cols), index=inverter_cols)

    # Irradiation
    daily_irr = df[irradiation_col].iloc[-1] if (irradiation_col and not df.empty) else 0
    monthly_irr = df[irradiation_col].mean() if (irradiation_col and not df.empty) else 0

    # Final DF
    final_df = pd.DataFrame({
        "Inverter": inverter_cols,
        "Daily Generation (kWh)": daily.values,
        "Monthly Generation (kWh)": monthly.values
    })

    return final_df, daily, monthly, daily_irr, monthly_irr



# ----------------------------
# KPIs
# ----------------------------
def calculate_kpis(customer, daily_generation, monthly_generation):
    num = CUSTOMER_INVERTERS[customer]
    base = PLF_BASE[customer]

    total_daily = daily_generation.sum()
    total_monthly = monthly_generation.sum()

    plf = total_daily / (24 * base * num)

    return total_daily, total_monthly, plf
