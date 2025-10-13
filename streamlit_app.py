import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from mongo_connector import fetch_cleaned_data

# --- Streamlit UI ---
st.set_page_config(page_title="DGR Generation Dashboard", layout="wide")
st.sidebar.image("enrich_logo.png", use_container_width=True)
st.sidebar.title("DGR Dashboard")

# --- Customer selection ---
CUSTOMER_TABLES = {
    "Imagica": "opcua_data",
    "BEL2": "BEL2",
    "Caspro": "Caspro",
    "Dunung": "Dunung",
    "Kasturi": "Kasturi",
    "Mauryaa": "Mauryaa",
    "Paranjape": "Paranjape",
    "Rajgir": "Rajgir",
    "Vinathi_3": "Vinathi_3",
    "Vinathi_4": "Vinathi_4",
    "TMD": "TMD",
    "PGCIL": "PGCIL"
}

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
    "TMD": 8,  # adjust based on TMD inverters
    "PGCIL": 32  # adjust based on PGCIL inverters
}

customer = st.sidebar.selectbox("Select Customer", list(CUSTOMER_TABLES.keys()))
collection_name = CUSTOMER_TABLES[customer]

# --- Date selection ---
selected_date = st.sidebar.date_input("Select Report Date", datetime.now().date())
report_date = selected_date
report_date_prev = report_date - timedelta(days=1)
month_start = report_date.replace(day=1)

# # --- Installed capacity input ---
# installed_capacity = st.sidebar.number_input(
#     "Enter Total Installed Capacity (kW)", min_value=1.0, value=990.0, step=10.0
# )

# --- Fetch data ---
start_str = month_start.strftime("%d-%b-%Y")
end_str = report_date.strftime("%d-%b-%Y")
st.info(f"Fetching data for {customer} from {start_str} to {end_str}...")

with st.spinner("Fetching data from MongoDB..."):
    df = fetch_cleaned_data(collection_name, start_str, end_str)

if df.empty:
    st.error("No data found for the selected range.")
    st.stop()

# --- Preprocess ---
df["day"] = pd.to_datetime(df["day"]).dt.strftime("%Y-%m-%d")
df["day_dt"] = pd.to_datetime(df["day"])
df = df.sort_values("day")

# Identify inverter columns
inverter_cols = [c for c in df.columns if "Daily_Generation" in c or c.startswith("T")]
irradiation_col = "Irradiation" if "Irradiation" in df.columns else None

# --- Daily row for previous day ---
daily_row = df[df["day"] == report_date_prev.strftime("%Y-%m-%d")]
if not daily_row.empty:
    daily_generation = daily_row[inverter_cols].iloc[0]
    daily_irradiation = daily_row[irradiation_col].iloc[0] if irradiation_col else 0
else:
    daily_generation = pd.Series([0]*len(inverter_cols), index=inverter_cols)
    daily_irradiation = 0

daily_df = pd.DataFrame({
    "Inverter": [f"Inverter-{i+1}" for i in range(len(inverter_cols))],
    "Daily Generation (kWh)": daily_generation.values
})

# --- Monthly generation till report date ---
month_df = df[df["day_dt"] <= report_date]
monthly_generation = month_df[inverter_cols].sum()
monthly_irradiation = month_df[irradiation_col].mean() if irradiation_col else 0

monthly_df = pd.DataFrame({
    "Inverter": [f"Inverter-{i+1}" for i in range(len(inverter_cols))],
    "Monthly Generation (kWh)": monthly_generation.values
})

# --- Combine daily & monthly ---
final_df = daily_df.merge(monthly_df, on="Inverter")

# Add Irradiation row
if irradiation_col:
    final_df = pd.concat([
        final_df,
        pd.DataFrame({
            "Inverter": ["Irradiation"],
            "Daily Generation (kWh)": [daily_irradiation],
            "Monthly Generation (kWh)": [monthly_irradiation]
        })
    ], ignore_index=True)

# --- KPI Metrics ---
total_daily_gen = daily_generation.values.sum()
total_monthly_gen = monthly_generation.sum()
num_inverters = CUSTOMER_INVERTERS.get(customer, len(inverter_cols))
plf_percent = (total_daily_gen / (24 * 3.04 * num_inverters)) * 100

col1, col2, col3 = st.columns(3)
col1.metric("Total Daily Generation (kWh)", f"{total_daily_gen:.2f}")
col2.metric("PLF % (Yesterday)", f"{plf_percent:.2f}%")
col3.metric("Total Monthly Generation (kWh)", f"{total_monthly_gen:.2f}")

# --- Display table ---
st.subheader(f"Generation Details for {report_date_prev.strftime('%d-%b-%Y')}")
st.dataframe(final_df.style.format({
    "Daily Generation (kWh)": "{:.2f}",
    "Monthly Generation (kWh)": "{:.2f}"
}), use_container_width=True)

# --- Excel export ---
safe_day_str = report_date_prev.strftime("%Y-%m-%d")
output_file = f"{customer}_DGR_Report_{safe_day_str}.xlsx"
with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    final_df.to_excel(writer, index=False, sheet_name="Generation Report")

with open(output_file, "rb") as f:
    st.download_button(
        "📥 Download Report as Excel",
        data=f,
        file_name=output_file,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
