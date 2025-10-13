# streamlit_app.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import pytz

from mongo_connector import fetch_cleaned_data
from dgr_generator import clean_dataframe, get_daily_monthly_data, calculate_kpis

IST = pytz.timezone("Asia/Kolkata")

# --- Customer tables mapping ---
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

# --- Streamlit page config ---
st.set_page_config(page_title="DGR Generation Dashboard", layout="wide")

# --- Sidebar ---
with st.sidebar:
    st.image("enrich_logo.png", use_column_width=True)  # replace with your logo file path
    st.title("DGR Dashboard Controls")
    
    # Customer selection
    customer = st.selectbox("Select Customer", list(CUSTOMER_TABLES.keys()))
    
    # Date selection
    selected_date = st.date_input("Select Report Date", datetime.now().date())
    
    # # Installed capacity input
    # installed_capacity = st.number_input(
    #     "Enter Total Installed Capacity (kW)", min_value=1.0, value=990.0, step=10.0
    # )

report_date = selected_date
month_start = report_date.replace(day=1)
collection_name = CUSTOMER_TABLES[customer]

# --- Fetch data ---
start_str = month_start.strftime("%d-%b-%Y")
end_str = report_date.strftime("%d-%b-%Y")
st.info(f"Fetching data for {customer} from {start_str} to {end_str}...")

with st.spinner("Fetching data from MongoDB..."):
    df = fetch_cleaned_data(collection_name, start_str, end_str)

if df.empty:
    st.error("No data found for this range.")
    st.stop()

# --- Process data ---
df, inverter_cols = clean_dataframe(df)
final_df, daily_generation, monthly_generation = get_daily_monthly_data(
    df, inverter_cols, month_start, report_date
)

total_daily_gen, total_monthly_gen, plf_percent = calculate_kpis(
    customer, daily_generation, monthly_generation)

# --- KPI cards ---
col1, col2, col3 = st.columns(3)
col1.metric("Total Daily Generation (kWh)", f"{total_daily_gen:.2f}")
col2.metric("PLF % (Yesterday)", f"{plf_percent:.2f}%")
col3.metric("Total Monthly Generation (kWh)", f"{total_monthly_gen:.2f}")

# --- Display table ---
st.subheader("Generation Details")
st.dataframe(
    final_df.style.format({
        "Daily Generation (kWh)": "{:.2f}",
        "Monthly Generation (kWh)": "{:.2f}"
    }),
    use_container_width=True
)

# --- Excel export ---
safe_day_str = (report_date - timedelta(days=1)).strftime("%Y-%m-%d")
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
