import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import pytz

from mongo_connector import fetch_cleaned_data
from dgr_generator import clean_dataframe, get_daily_monthly_data, calculate_kpis

IST = pytz.timezone("Asia/Kolkata")

CUSTOMER_TABLES = {
    "Imagica": "opcua_data",
    "BEL2": "BEL2",
    "Caspro": "Caspro",
    "Dunung": "Dunung",
    "Kasturi": "Kasturi",
    "Mauryaa": "Mauryaa",
    "Paranjape": "Paranjape",
    "BEL1": "Rajgir",
    "Vinathi_3": "Vinathi_3",
    "Vinathi_4": "Vinathi_4",
    "TMD": "TMD",
    "PGCIL": "PGCIL"
}

st.set_page_config(page_title="DGR Generation Dashboard", layout="wide")

with st.sidebar:
    st.image("enrich_logo.png", width="stretch")
    st.title("DGR Dashboard Controls")
    customer = st.selectbox("Select Customer", list(CUSTOMER_TABLES.keys()))
    selected_date = st.date_input("Select Report Date", datetime.now().date())

report_date = selected_date
month_start = report_date.replace(day=1)
collection_name = CUSTOMER_TABLES[customer]

start_str = month_start.strftime("%d-%b-%Y")
end_str = report_date.strftime("%d-%b-%Y")
st.info(f"Fetching data for **{customer}** from {start_str} to {end_str}...")

with st.spinner("Fetching data from MongoDB..."):
    df = fetch_cleaned_data(collection_name, start_str, end_str, customer)

if df.empty:
    st.error("No data found for this range.")
    st.stop()

df, inverter_cols, irradiation_col = clean_dataframe(df, customer)

final_df, daily_generation, monthly_generation, daily_irradiation, monthly_avg_irradiation = get_daily_monthly_data(
    df, inverter_cols, month_start, report_date, irradiation_col, customer
)

total_daily_gen, total_monthly_gen, plf_percent = calculate_kpis(customer, daily_generation, monthly_generation)

# KPI Cards
col1, col2, col3 = st.columns(3)
col1.metric("Total Daily Generation (kWh)", f"{total_daily_gen:.2f}")
col2.metric("PLF % (Yesterday)", f"{plf_percent:.2f}%")
col3.metric("Total Monthly Generation (kWh)", f"{total_monthly_gen:.2f}")

if daily_irradiation is not None or monthly_avg_irradiation is not None:
    col4, col5 = st.columns(2)
    if daily_irradiation is not None:
        col4.metric("Daily Irradiation (kWh/m²)", f"{daily_irradiation:.2f}")
    if monthly_avg_irradiation is not None:
        col5.metric("Avg Monthly Irradiation (kWh/m²)", f"{monthly_avg_irradiation:.2f}")

st.subheader("Inverter-wise Generation Details")
st.dataframe(
    final_df.style.format({
        "Daily Generation (kWh)": "{:.2f}",
        "Monthly Generation (kWh)": "{:.2f}"
    }),
    width="stretch"
)

safe_day_str = (report_date - timedelta(days=1)).strftime("%Y-%m-%d")
output_file = f"{customer}_DGR_Report_{safe_day_str}.xlsx"

with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    final_df.to_excel(writer, index=False, sheet_name="Generation Report")
    summary_data = {
        "Metric": ["Customer", "Report Date (Yesterday)", "Total Daily Generation (kWh)",
                   "Total Monthly Generation (kWh)", "PLF (%)"],
        "Value": [customer, safe_day_str, round(total_daily_gen, 2),
                  round(total_monthly_gen, 2), round(plf_percent, 2)]
    }
    if daily_irradiation is not None:
        summary_data["Metric"].append("Daily Irradiation (kWh/m²)")
        summary_data["Value"].append(round(daily_irradiation, 2))
    if monthly_avg_irradiation is not None:
        summary_data["Metric"].append("Avg Monthly Irradiation (kWh/m²)")
        summary_data["Value"].append(round(monthly_avg_irradiation, 2))

    summary_df = pd.DataFrame(summary_data)
    summary_df.to_excel(writer, index=False, sheet_name="Summary")

with open(output_file, "rb") as f:
    st.download_button(
        "📥 Download DGR Report (Excel)",
        data=f,
        file_name=output_file,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

