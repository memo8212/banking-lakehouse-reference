"""Banking lakehouse — Gold mart explorer (Streamlit).

    pip install -r requirements-streamlit.txt
    python -m lakehouse run            # build the gold marts first
    streamlit run app.py
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from lakehouse.read import list_gold, read_gold

st.set_page_config(page_title="Banking Lakehouse — Gold", layout="wide")
st.title("🏦 Banking Lakehouse — Gold explorer")
st.caption("Explore the consumer-ready marts (customer_360, payments, loans, "
           "accounts, risk, AML). Data is read from the local Delta store.")

storage = st.sidebar.text_input("Storage", "lakehouse_storage")
tables = list_gold(storage)

if not tables:
    st.warning("No gold marts found. Build them first:\n\n"
               "```\npython data/generate_sample_data.py\npython -m lakehouse run\n```")
    st.stop()

table = st.sidebar.selectbox("Gold mart", tables)
df = read_gold(storage, table)

c1, c2, c3 = st.columns(3)
c1.metric("Rows", f"{len(df):,}")
c2.metric("Columns", len(df.columns))
c3.metric("Marts available", len(tables))

st.subheader(f"`gold.{table}`")
st.dataframe(df, use_container_width=True, hide_index=True)

# quick chart: sum a numeric column grouped by a categorical column
cats = [c for c in df.columns if df[c].dtype == object]
nums = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
if cats and nums:
    st.subheader("Chart")
    a, b = st.columns(2)
    group = a.selectbox("Group by", cats)
    value = b.selectbox("Value (summed)", nums)
    agg = (df.groupby(group, as_index=True)[value].sum()
             .sort_values(ascending=False))
    st.bar_chart(agg)

st.download_button("⬇️ Download CSV", df.to_csv(index=False),
                   file_name=f"gold_{table}.csv")
