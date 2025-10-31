import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import pytz
from streamlit_autorefresh import st_autorefresh

from mongo_connector import fetch_cleaned_data
from dgr_generator import clean_dataframe, calculate_kpis

IST = pytz.timezone("Asia/Kolkata")
st.set_page_config(page_title="DGR Generation Dashboard", layout="wide")

CUSTOMER_TABLES = {
    "Imagica": "opcua_data",
    "BEL2": "BEL2",
    "Caspro": "Caspro",
    "Dunung": "Dunung",
    "Kasturi": "Kasturi",
    "Mauryaa": "Mauryaa",
    "Paranjape": "Paranjape",
    "BEL1": "Rajgir",
    "Vinathi_2": "PSS",
    "Vinathi_3": "Vinathi_3",
    "Vinathi_4": "Vinathi_4",
    "TMD": "TMD",
    "PGCIL": "PGCIL",
}

# -------------------------------------------------------
# Sidebar
# -------------------------------------------------------
if "page" not in st.session_state:
    st.session_state.page = "report"

with st.sidebar:
    st.image("enrich_logo.png")
    st.title("DGR Dashboard Controls")
    if st.button("📊 DGR Customer Report"):
        st.session_state.page = "report"
    if st.button("🔴 Live Generation Data"):
        st.session_state.page = "live"

# -------------------------------------------------------
# Date Controls
# -------------------------------------------------------
today_date = datetime.now().date()
month_start = today_date.replace(day=1)
report_date = today_date + timedelta(days=1)

# -------------------------------------------------------
# ✅ LIVE PAGE (Option 1)
# -------------------------------------------------------
if st.session_state.page == "live":
    st.title("🔴 Live Generation Data (All Customers)")
    st.caption("Latest generation and irradiation data for all customers.")
    st_autorefresh(interval=60_000, limit=None, key="live_refresh")

    summary_list = []
    total_daily_all = 0

    for cust, coll in CUSTOMER_TABLES.items():
        df = fetch_cleaned_data(
            coll,
            month_start.strftime("%d-%b-%Y"),
            report_date.strftime("%d-%b-%Y"),
            cust,
            live_mode=True,  # only latest row
        )

        if df.empty:
            summary_list.append(
                {"Plant": cust, "Total Daily Generation (kWh)": 0, "PLF (%)": 0, "Irradiation (kWh/m²)": 0}
            )
            continue

        df, inverter_cols, irradiation_col = clean_dataframe(df, cust)

        if not inverter_cols:
            summary_list.append(
                {"Plant": cust, "Total Daily Generation (kWh)": 0, "PLF (%)": 0, "Irradiation (kWh/m²)": 0}
            )
            continue

        latest_row = df.iloc[-1]
        daily_gen = latest_row[inverter_cols]
        monthly_gen = daily_gen  # not relevant for live
        daily_irr = latest_row[irradiation_col] if irradiation_col else 0

        total_daily, _, plf = calculate_kpis(cust, daily_gen, monthly_gen)
        total_daily_all += total_daily

        summary_list.append(
            {
                "Plant": cust,
                "Total Daily Generation (kWh)": round(total_daily, 2),
                "PLF (%)": round(plf, 2),
                "Irradiation (kWh/m²)": round(daily_irr, 2),
            }
        )

    st.metric("Total Generation of All Customers (kWh)", f"{round(total_daily_all, 2)}")
    summary_df = pd.DataFrame(summary_list)
    st.dataframe(summary_df)
    st.caption(f"Last refreshed at {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST")
