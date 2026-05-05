"""
DataMind Streamlit UI.

Pages:
- 📤 Upload    — ingest CSV/Excel datasets
- 💬 Ask       — natural language queries (NL→SQL)
- 📊 Metrics   — MRR, churn, ARPU trend charts
- 🚨 Anomalies — detected anomalies with LLM explanations
- 📄 Reports   — generate and download PDF reports
- ❤️ Health    — system status dashboard
"""
import io
import time
import httpx
import pandas as pd
import streamlit as st

API_BASE = "http://localhost:8000/api/v1"

st.set_page_config(
    page_title="DataMind",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.metric-card { background:#f8f9fa; border-radius:8px; padding:14px 18px; border:1px solid #e9ecef; }
.badge-high   { background:#fde8e8; color:#c0392b; padding:2px 8px; border-radius:10px; font-size:.8em; font-weight:bold; }
.badge-medium { background:#fef3e2; color:#e67e22; padding:2px 8px; border-radius:10px; font-size:.8em; font-weight:bold; }
.badge-low    { background:#e8f8ee; color:#27ae60; padding:2px 8px; border-radius:10px; font-size:.8em; font-weight:bold; }
</style>
""", unsafe_allow_html=True)

def api_get(path: str, params: dict | None = None) -> dict | None:
    try:
        r = httpx.get(f"{API_BASE}{path}", params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
        return None


def api_post(path: str, json: dict | None = None, files=None, data=None) -> dict | None:
    try:
        r = httpx.post(f"{API_BASE}{path}", json=json, files=files, data=data, timeout=60)
        r.raise_for_status()
        return r.json()
    except httpx.HTTPStatusError as e:
        detail = e.response.json().get("detail", str(e))
        st.error(f"Error: {detail}")
        return None
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None


@st.cache_data(ttl=15)
def get_datasets() -> list[dict]:
    result = api_get("/ingest/datasets")
    return result.get("datasets", []) if result else []


@st.cache_data(ttl=30)
def get_health() -> dict | None:
    return api_get("/health")

with st.sidebar:
    st.title("📊 DataMind")
    st.caption("Intelligent Business ETL + LLM")

    health = get_health()
    if health:
        icon = "🟢" if health["status"] == "ok" else "🟡"
        st.markdown(f"{icon} API **{health['status']}** · {health['active_datasets']} datasets")
    else:
        st.markdown("🔴 API unreachable")

    st.divider()
    page = st.radio(
        "Navigation",
        ["📤 Upload", "💬 Ask", "📊 Metrics", "🚨 Anomalies", "📄 Reports", "❤️ Health"],
        label_visibility="collapsed",
    )

if page == "📤 Upload":
    st.header("Upload dataset")
    st.caption("Supported: CSV, Excel (.xlsx/.xls), TSV")

    col1, col2 = st.columns([2, 1])
    with col1:
        uploaded = st.file_uploader("Drop your file here", type=["csv", "xlsx", "xls", "tsv"])
    with col2:
        dataset_name = st.text_input("Dataset name", placeholder="saas_q1_2024")
        domain = st.selectbox("Domain", ["saas"])

    if uploaded and dataset_name and st.button("Run ETL pipeline", type="primary"):
        file_bytes = uploaded.getvalue()
        mime = "text/csv" if uploaded.name.endswith(".csv") else "application/octet-stream"

        with st.spinner("Queuing..."):
            result = api_post(
                "/ingest/csv",
                files={"file": (uploaded.name, io.BytesIO(file_bytes), mime)},
                data={"dataset_name": dataset_name, "domain": domain},
            )

        if result:
            job_id = result["job_id"]
            st.success(f"Queued! Job ID: `{job_id}`")
            bar = st.progress(0, text="Starting ETL pipeline...")

            for _ in range(300):
                job = api_get(f"/ingest/jobs/{job_id}")
                if not job:
                    break
                bar.progress(job["progress"] / 100, text=f"Stage: **{job['stage']}** ({job['progress']}%)")
                if job["status"] in ("completed", "failed"):
                    break
                time.sleep(2)

            if job and job["status"] == "completed":
                st.success(f"✅ ETL complete! {job['rows_cleaned']:,} rows processed, {job['rows_enriched']:,} enriched by LLM.")
                st.cache_data.clear()
            elif job:
                st.error(f"❌ ETL failed: {job.get('error_message', 'unknown error')}")

    # Show existing datasets
    st.divider()
    st.subheader("Existing datasets")
    datasets = get_datasets()
    if not datasets:
        st.info("No datasets yet.")
    for d in datasets:
        status_icon = {"ready": "✅", "processing": "⏳", "failed": "❌", "pending": "🕐"}.get(d["status"], "❓")
        with st.expander(f"{status_icon} **{d['name']}** — {d['row_count']:,} rows · {d['status']}"):
            c1, c2 = st.columns(2)
            c1.markdown(f"**ID:** `{d['id']}`")
            c1.markdown(f"**Domain:** {d['domain']}")
            c2.markdown(f"**Columns:** {d['column_count']}")
            c2.markdown(f"**Created:** {d['created_at'][:10]}")
            if d.get("error_message"):
                st.error(d["error_message"])
            if st.button("🗑 Delete", key=f"del_{d['id']}"):
                httpx.delete(f"{API_BASE}/ingest/datasets/{d['id']}", timeout=10)
                st.cache_data.clear()
                st.rerun()

elif page == "💬 Ask":
    st.header("Ask your data")
    st.caption("Natural language → SQL → business answer")

    datasets = [d for d in get_datasets() if d["status"] == "ready"]
    if not datasets:
        st.info("No ready datasets. Upload and process a dataset first.")
    else:
        selected = st.selectbox("Dataset", datasets, format_func=lambda d: d["name"])

        # Example questions
        st.markdown("**Example questions:**")
        examples = [
            "What is the total MRR from active customers?",
            "Which plan has the highest churn rate?",
            "Show me the top 5 customers by MRR",
            "What percentage of customers churned due to price?",
            "What is the average lifetime of churned customers in months?",
            "Which country has the most active customers?",
        ]
        cols = st.columns(3)
        chosen_example = None
        for i, ex in enumerate(examples):
            if cols[i % 3].button(ex, key=f"ex_{i}"):
                chosen_example = ex

        question = st.text_area(
            "Your question",
            value=chosen_example or "",
            height=80,
            placeholder="What is the monthly churn rate for enterprise customers?",
        )

        if st.button("Ask", type="primary", disabled=not question.strip()):
            with st.spinner("Generating SQL and fetching answer..."):
                result = api_post("/query", json={
                    "question": question.strip(),
                    "dataset_id": selected["id"],
                })

            if result:
                st.markdown("### Answer")
                st.markdown(result["answer"])

                col1, col2, col3 = st.columns(3)
                col1.metric("Rows returned", result["row_count"])
                col2.metric("Latency", f"{result['latency_ms']:.0f} ms")
                col3.metric("Success", "✓" if result["success"] else "✗")

                with st.expander("Generated SQL", expanded=False):
                    st.code(result["sql"], language="sql")

                if result["result"]:
                    st.markdown("### Data")
                    st.dataframe(pd.DataFrame(result["result"]), use_container_width=True)

        # Query history
        st.divider()
        with st.expander("Query history"):
            history = api_get("/query/history", params={"limit": 10})
            if history:
                for h in history:
                    icon = "✓" if h["success"] else "✗"
                    st.markdown(f"**{icon} {h['question']}**")
                    st.caption(f"SQL: `{(h['sql'] or '')[:80]}...` · {h['row_count']} rows · {h['latency_ms']:.0f}ms")
                    st.divider()

elif page == "📊 Metrics":
    st.header("SaaS metrics dashboard")

    datasets = [d for d in get_datasets() if d["status"] == "ready"]
    if not datasets:
        st.info("No ready datasets.")
    else:
        selected = st.selectbox("Dataset", datasets, format_func=lambda d: d["name"])
        summary = api_get(f"/insights/summary/{selected['id']}")
        metrics = api_get("/insights/metrics", params={"dataset_id": selected["id"], "periods": 12})

        if summary:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("MRR", f"${summary['mrr']:,.0f}")
            c2.metric("Active Customers", f"{summary['active_customers']:,}")
            c3.metric("Churn Rate", f"{summary['churn_rate']:.1%}")
            c4.metric("Net New MRR", f"${summary['net_new_mrr']:+,.0f}")

        if metrics and metrics.get("snapshots"):
            df = pd.DataFrame(metrics["snapshots"])
            df["period"] = pd.to_datetime(df["period"])

            st.divider()
            tab1, tab2, tab3 = st.tabs(["MRR Trend", "Churn Rate", "Customer Growth"])

            with tab1:
                import plotly.graph_objects as go
                fig = go.Figure()
                fig.add_trace(go.Bar(x=df["period"], y=df["new_mrr"], name="New MRR", marker_color="#27ae60"))
                fig.add_trace(go.Bar(x=df["period"], y=-df["churned_mrr"], name="Churned MRR", marker_color="#e74c3c"))
                fig.add_trace(go.Scatter(x=df["period"], y=df["mrr"], name="Total MRR", line=dict(color="#3498db", width=2)))
                fig.update_layout(barmode="relative", title="MRR Waterfall", height=350)
                st.plotly_chart(fig, use_container_width=True)

            with tab2:
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(
                    x=df["period"], y=df["churn_rate"] * 100,
                    fill="tozeroy", line=dict(color="#e74c3c"),
                    name="Churn Rate %",
                ))
                fig2.update_layout(title="Monthly Churn Rate (%)", yaxis_ticksuffix="%", height=350)
                st.plotly_chart(fig2, use_container_width=True)

            with tab3:
                fig3 = go.Figure()
                fig3.add_trace(go.Scatter(x=df["period"], y=df["active_customers"], name="Active", line=dict(color="#3498db")))
                fig3.add_trace(go.Bar(x=df["period"], y=df["new_customers"], name="New", marker_color="#27ae60", opacity=0.6))
                fig3.add_trace(go.Bar(x=df["period"], y=-df["churned_customers"], name="Churned", marker_color="#e74c3c", opacity=0.6))
                fig3.update_layout(barmode="relative", title="Customer Movement", height=350)
                st.plotly_chart(fig3, use_container_width=True)

            st.subheader("Full data table")
            st.dataframe(df.sort_values("period", ascending=False), use_container_width=True)

elif page == "🚨 Anomalies":
    st.header("Anomaly detection")
    st.caption("Z-score + IQR detection with LLM business explanation")

    datasets = [d for d in get_datasets() if d["status"] == "ready"]
    if not datasets:
        st.info("No ready datasets.")
    else:
        col1, col2 = st.columns([2, 1])
        with col1:
            selected = st.selectbox("Dataset", datasets, format_func=lambda d: d["name"])
        with col2:
            severity_filter = st.selectbox("Severity filter", ["all", "high", "medium", "low"])

        params = {"dataset_id": selected["id"], "limit": 100}
        if severity_filter != "all":
            params["severity"] = severity_filter

        data = api_get("/insights/anomalies", params=params)
        if data:
            anomalies = data["anomalies"]
            st.markdown(f"**{data['total']} anomalies detected**")

            counts = {"high": 0, "medium": 0, "low": 0}
            for a in anomalies:
                counts[a["severity"]] = counts.get(a["severity"], 0) + 1

            c1, c2, c3 = st.columns(3)
            c1.metric("🔴 High", counts["high"])
            c2.metric("🟡 Medium", counts["medium"])
            c3.metric("🟢 Low", counts["low"])

            st.divider()
            for a in anomalies:
                badge_class = f"badge-{a['severity']}"
                with st.expander(
                    f"[{a['severity'].upper()}] {a['column_name']} = {a['value']:.2f} (score: {a['score']:.2f})"
                ):
                    st.markdown(f"**Detection method:** `{a['method']}`")
                    st.markdown(f"**Anomaly score:** {a['score']:.4f}")
                    if a.get("llm_explanation"):
                        st.info(f"💡 **LLM Explanation:** {a['llm_explanation']}")
                    else:
                        st.caption("No LLM explanation available.")

elif page == "📄 Reports":
    st.header("Generate PDF report")
    st.caption("LLM-powered narrative SaaS performance report")

    datasets = [d for d in get_datasets() if d["status"] == "ready"]
    if not datasets:
        st.info("No ready datasets.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            selected = st.selectbox("Dataset", datasets, format_func=lambda d: d["name"])
        with col2:
            period = st.text_input("Period (YYYY-MM)", value=pd.Timestamp.now().strftime("%Y-%m"))

        if st.button("Generate report", type="primary"):
            with st.spinner("Generating narrative report with LLM... (~30 seconds)"):
                result = api_post("/insights/report", json={
                    "dataset_id": selected["id"],
                    "period": period,
                })

            if result and result.get("success"):
                st.success(f"✅ Report generated! Tokens used: {result['tokens_used']:,}")

                st.subheader("Report preview")
                st.markdown(result["markdown"])

                if result.get("pdf_url"):
                    pdf_url = f"http://localhost:8000{result['pdf_url']}"
                    st.markdown(f"📥 [**Download PDF**]({pdf_url})")
            elif result:
                st.error(result.get("error", "Report generation failed."))

elif page == "❤️ Health":
    st.header("System health")

    if st.button("Refresh"):
        st.cache_data.clear()
        st.rerun()

    health = get_health()
    if not health:
        st.error("Cannot reach API. Make sure the server is running.")
        st.code("uvicorn src.api.main:app --reload")
    else:
        st.markdown(f"**Version:** `{health['version']}` · **Environment:** `{health['environment']}`")

        st.subheader("Components")
        cols = st.columns(len(health["components"]))
        for col, (name, status) in zip(cols, health["components"].items()):
            icon = "✅" if status == "ok" else "❌"
            col.metric(name.upper(), f"{icon} {status}")

        st.subheader("Quick links")
        st.markdown("""
| Service | URL |
|---|---|
| API Swagger | http://localhost:8000/docs |
| Prometheus metrics | http://localhost:8000/metrics |
| Grafana dashboard | http://localhost:3000 |
| Flower (Celery) | http://localhost:5555 |
| MLflow | http://localhost:5000 |
""")
        st.code("docker compose up --build", language="bash")
