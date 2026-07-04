import os
import sqlite3
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st

SQLITE_DB_PATH = os.environ.get("SQLITE_DB_PATH", "/data/analytics.sqlite3")
REFRESH_INTERVAL = int(os.environ.get("REFRESH_INTERVAL_SECONDS", "30"))

CATEGORY_COLORS = {
    "Sports": "#e74c3c",
    "Science": "#3498db",
    "Technology": "#2ecc71",
    "Politics": "#9b59b6",
    "History": "#f39c12",
    "Arts": "#e91e63",
    "Entertainment": "#ff9800",
    "Geography": "#00bcd4",
    "Business": "#27ae60",
    "Health": "#16a085",
    "Military": "#7f8c8d",
    "Religion": "#8d6e63",
    "Others": "#bdc3c7",
}

st.set_page_config(
    page_title="Wikimedia Trending Topics",
    page_icon="📖",
    layout="wide",
)

st.markdown("""
<style>
[data-testid="stMetric"] {
    border-radius: 0.5rem;
    padding: 0.75rem 1rem;
    border-left: 4px solid #4a90d9;
    background: rgba(74, 144, 217, 0.08);
}
</style>
""", unsafe_allow_html=True)


def get_conn():
    if not os.path.exists(SQLITE_DB_PATH):
        return None
    return sqlite3.connect(SQLITE_DB_PATH, check_same_thread=False)


def query_latest_metrics(conn):
    try:
        return pd.read_sql_query(
            """
            SELECT category, count, trend_score, window_start, window_end
            FROM window_metrics
            WHERE window_start = (SELECT MAX(window_start) FROM window_metrics)
            ORDER BY count DESC
            """,
            conn,
        )
    except Exception:
        return pd.DataFrame()


def query_history(conn, n_windows=24):
    try:
        return pd.read_sql_query(
            """
            SELECT window_start, category, count
            FROM window_metrics
            WHERE window_start IN (
                SELECT DISTINCT window_start FROM window_metrics
                ORDER BY window_start DESC
                LIMIT ?
            )
            ORDER BY window_start ASC
            """,
            conn,
            params=(n_windows,),
        )
    except Exception:
        return pd.DataFrame()


def query_recent_events(conn, limit=50):
    try:
        return pd.read_sql_query(
            """
            SELECT timestamp, title, category
            FROM events
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            conn,
            params=(limit,),
        )
    except Exception:
        return pd.DataFrame()


# ── Header (renderizado uma vez, fora do fragment) ───────────────────────────
col_title, col_btn = st.columns([5, 1])
with col_title:
    st.title("📖 Wikimedia Trending Topics")
    st.caption("Streaming analysis of Wikipedia edits, semantically classified by topic.")
with col_btn:
    st.write("")
    if st.button("🔄 Refresh"):
        st.rerun()


# ── Dados (fragment: auto-atualiza sem bloquear o botão) ─────────────────────
@st.fragment(run_every=REFRESH_INTERVAL)
def render_data():
    conn = get_conn()

    if conn is None:
        st.warning(
            f"Database not found at `{SQLITE_DB_PATH}`. "
            "Make sure the Spark consumer service is running."
        )
        return

    latest = query_latest_metrics(conn)
    history = query_history(conn)
    recent_events = query_recent_events(conn)
    conn.close()

    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    st.caption(f"Last refreshed: **{now_str}** · auto-refresh every {REFRESH_INTERVAL}s")

    if latest.empty:
        st.info(
            "No data yet. The Spark consumer is running — "
            "results appear after the first processing window completes."
        )
        return

    # ── KPI metrics ──────────────────────────────────────────────────────────
    total_edits = int(latest["count"].sum())
    top_cat = latest.iloc[0]["category"]
    top_cat_pct = round(latest.iloc[0]["count"] / total_edits * 100, 1)
    num_cats = latest["category"].nunique()
    win_label = "{} → {}".format(
        latest["window_start"].iloc[0][:16],
        latest["window_end"].iloc[0][:16],
    )

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Edits", f"{total_edits:,}")
    m2.metric("Top Category", top_cat, f"{top_cat_pct}% of edits")
    m3.metric("Active Categories", num_cats)
    m4.metric("Current Window", win_label)

    st.divider()

    # ── Bar + Pie ─────────────────────────────────────────────────────────────
    c1, c2 = st.columns([3, 2])

    with c1:
        fig_bar = px.bar(
            latest,
            x="count",
            y="category",
            orientation="h",
            title="Edits per Category — Current Window",
            color="category",
            color_discrete_map=CATEGORY_COLORS,
            text="count",
        )
        fig_bar.update_layout(
            showlegend=False,
            yaxis=dict(categoryorder="total ascending"),
            margin=dict(l=10, r=30, t=40, b=10),
            height=400,
        )
        fig_bar.update_traces(textposition="outside")
        st.plotly_chart(fig_bar, use_container_width=True)

    with c2:
        fig_pie = px.pie(
            latest,
            values="count",
            names="category",
            title="Distribution",
            color="category",
            color_discrete_map=CATEGORY_COLORS,
            hole=0.38,
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        fig_pie.update_layout(
            showlegend=False,
            margin=dict(l=10, r=10, t=40, b=10),
            height=400,
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    # ── Trend Score ───────────────────────────────────────────────────────────
    if "trend_score" in latest.columns and latest["trend_score"].sum() > 0:
        st.subheader("Popularity Score — Current Window")
        st.caption("Relative activity within the window (100 = most edited category). Does not measure growth vs. historical average.")
        fig_trend = px.bar(
            latest.sort_values("trend_score"),
            x="trend_score",
            y="category",
            orientation="h",
            color="category",
            color_discrete_map=CATEGORY_COLORS,
            range_x=[0, 105],
            text=latest.sort_values("trend_score")["trend_score"].apply(lambda v: f"{v:.0f}"),
        )
        fig_trend.update_layout(
            showlegend=False,
            margin=dict(l=10, r=10, t=10, b=10),
            height=300,
            xaxis_title="Popularity Score",
            yaxis_title="",
        )
        fig_trend.update_traces(textposition="outside")
        st.plotly_chart(fig_trend, use_container_width=True)

    # ── Historical line chart ─────────────────────────────────────────────────
    if not history.empty:
        st.subheader("Category Trends Over Time")
        history["window_start"] = pd.to_datetime(history["window_start"])
        fig_line = px.line(
            history,
            x="window_start",
            y="count",
            color="category",
            color_discrete_map=CATEGORY_COLORS,
            markers=True,
            labels={"window_start": "Window", "count": "Edits", "category": "Category"},
        )
        fig_line.update_layout(
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=10, r=10, t=50, b=10),
            height=350,
        )
        st.plotly_chart(fig_line, use_container_width=True)

    # ── Recent edits table ────────────────────────────────────────────────────
    st.subheader("Recent Edits")
    if not recent_events.empty:
        st.dataframe(
            recent_events.rename(
                columns={"timestamp": "Timestamp", "title": "Article", "category": "Category"}
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No individual edit records yet.")


render_data()
