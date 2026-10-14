"""Banking lakehouse — Ask (Genie) over the gold marts.

    pip install -r requirements-streamlit.txt
    python -m lakehouse run
    streamlit run app_genie.py
"""
from __future__ import annotations

import streamlit as st

from semantic.ask import answer
from semantic.execute import execute
from semantic.model import load_model

st.set_page_config(page_title="Banking Lakehouse — Ask", layout="wide")
st.title("🧞 Banking Lakehouse — Ask")
st.caption("Natural-language questions over the gold marts, backed by the semantic layer "
           "(Genie-style). Offline heuristic matching; an LLM is used if configured.")

model = load_model()
storage = st.sidebar.text_input("Storage", "lakehouse_storage")
st.sidebar.metric("Metrics", len(model.metrics))

questions = [m.question for m in model.metrics.values() if m.question]
choice = st.selectbox("Suggested questions", ["(type your own)"] + questions)
default = "" if choice == "(type your own)" else choice
q = st.text_input("Ask a question", value=default)

if q:
    res = answer(q, model)
    if not res["metric"]:
        st.warning("No matching metric — try a suggested question.")
    else:
        gb = res.get("group_by")
        st.success(f"metric: **{res['metric']}**  ·  source={res['source']}"
                   + (f"  ·  group by **{gb}**" if gb else ""))
        st.code(res["sql"], language="sql")
        df = execute(storage, model, model.get(res["metric"]), group_by=gb)
        if df.empty:
            st.info("No data — run `python -m lakehouse run` to build the gold marts.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
            if gb and len(df) > 1:
                st.bar_chart(df.set_index(gb)[res["metric"]])
