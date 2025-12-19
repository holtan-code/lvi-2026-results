import math

import pandas as pd
import streamlit as st
import altair as alt
from sqlalchemy import create_engine, text

# -----------------------------------------------------------------------------
# PAGE CONFIG
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="LVI 2026 Brand Dashboard",
    layout="wide",
)

st.title("LVI 2026 Brand Dashboard")

# -----------------------------------------------------------------------------
# DIRECT DB CONNECTION (HARD-CODED URL)
# -----------------------------------------------------------------------------
db_url = (
    "postgresql://doadmin:LjdMqa1UVxbUbvdF@"
    "lmbr-do-user-10682395-0.b.db.ondigitalocean.com:25060/defaultdb"
    "?sslmode=require"
)

engine = create_engine(db_url)

# -----------------------------------------------------------------------------
# SQL QUERIES
# -----------------------------------------------------------------------------
QUERIES = {
    "Top 100 Brands Overall": """
        SELECT
            s.*,
            r.rank,
            p.platform_a,
            p.platform_b
        FROM lvi_2026_brand_scores_ch s
        JOIN lvi_2026_ranking_ch r
          ON s.brand_id = r.brand_id
        LEFT JOIN lvi_2024_brand_national_location_count m
          ON s.brand_id = m.brandid
        LEFT JOIN "VOID-lvi_2026_brand_platform_list-12-18-2025" p
          ON s.brand_id = p.brandid
        ORDER BY r.rank
        LIMIT 100;
    """,

    "Enterprise Brands (All)": """
        SELECT
            s.brand_id,
            s.brandname,
            s.category,
            s.child_category,
            m.total_us_locations,
            s.optimization_score,
            s.performance_score,
            s.search_score,
            s.reputation_score,
            s.social_score,
            s.ai_overall_score,
            s.ranking_value,
            p.platform_a,
            p.platform_b
        FROM lvi_2026_brand_scores_ch s
        JOIN lvi_2024_brand_national_location_count m
          ON s.brand_id = m.brandid
        LEFT JOIN lvi_2026_brand_platform_list p
          ON m.brandid = p.brandid
        WHERE m.total_us_locations >= 500
        ORDER BY s.ranking_value DESC;
    """,

    "AI Enterprise": """
        SELECT
            s.brand_id,
            s.brandname,
            s.category,
            s.child_category,
            m.total_us_locations,
            p.platform_a,
            p.platform_b,
            s.ai_overall_score,
            s.optimization_score,
            s.performance_score,
            s.search_score,
            s.reputation_score,
            s.social_score
        FROM lvi_2026_brand_scores_ch s
        JOIN lvi_2024_brand_national_location_count m
          ON s.brand_id = m.brandid
        LEFT JOIN lvi_2026_brand_platform_list p
          ON m.brandid = p.brandid
        WHERE m.total_us_locations >= 500
        ORDER BY s.ai_overall_score DESC;
    """,

    "Food Enterprise": """
        SELECT
            s.*,
            m.total_us_locations,
            p.platform_a,
            p.platform_b
        FROM lvi_2026_brand_scores_ch s
        JOIN lvi_2024_us_locations m
          ON s.brand_id = m.brandid
        LEFT JOIN lvi_2026_brand_platform_list p
          ON s.brand_id = p.brandid
        WHERE s.category LIKE 'Food%%'
          AND m.total_us_locations >= 500
        ORDER BY s.ranking_value DESC;
    """,

    "Retail Enterprise": """
        SELECT
            s.*,
            m.total_us_locations,
            p.platform_a,
            p.platform_b
        FROM lvi_2026_brand_scores_ch s
        JOIN lvi_2024_us_locations m
          ON s.brand_id = m.brandid
        LEFT JOIN lvi_2026_brand_platform_list p
          ON s.brand_id = p.brandid
        WHERE s.category LIKE 'Retail%%'
          AND m.total_us_locations >= 500
        ORDER BY s.ranking_value DESC;
    """,

    "Financial Services Enterprise": """
        SELECT
            s.*,
            m.total_us_locations,
            p.platform_a,
            p.platform_b
        FROM lvi_2026_brand_scores_ch s
        JOIN lvi_2024_us_locations m
          ON s.brand_id = m.brandid
        LEFT JOIN lvi_2026_brand_platform_list p
          ON s.brand_id = p.brandid
        WHERE s.category LIKE 'Fin%%'
          AND m.total_us_locations >= 500
        ORDER BY s.ranking_value DESC;
    """,
}

# -----------------------------------------------------------------------------
# DB / QUERY HELPER
# -----------------------------------------------------------------------------
def run_query(sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)

    # Round all numeric columns to 2 decimal places
    numeric_cols = df.select_dtypes(include=["number"]).columns
    if len(numeric_cols) > 0:
        df[numeric_cols] = df[numeric_cols].round(2)

    return df

# -----------------------------------------------------------------------------
# GENERIC CHART HELPER (USED BY *ALL* VIEWS)
# -----------------------------------------------------------------------------
def show_brand_chart(
    df: pd.DataFrame,
    metric_col: str,
    session_key: str,
    title_prefix: str,
    page_size: int = 20,
):
    """Paginated bar chart of metric_col by brandname at top of the view."""
    if "brandname" not in df.columns or metric_col not in df.columns:
        return

    total_rows = len(df)
    if total_rows == 0:
        return

    total_pages = math.ceil(total_rows / page_size)

    if session_key not in st.session_state:
        st.session_state[session_key] = 1

    page = st.session_state[session_key]

    # Clamp page
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages
    st.session_state[session_key] = page

    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    page_df = df.iloc[start_idx:end_idx].copy()

    chart_data = page_df[["brandname", metric_col]]

    chart = (
        alt.Chart(chart_data)
        .mark_bar()
        .encode(
            x=alt.X(f"{metric_col}:Q", title=title_prefix),
            y=alt.Y("brandname:N", sort="-x", title="Brand"),
            tooltip=[
                alt.Tooltip("brandname:N", title="Brand"),
                alt.Tooltip(f"{metric_col}:Q", title=title_prefix),
            ],
        )
        .properties(
            height=max(200, 20 * len(chart_data)),
            width="container",
            title=(
                f"{title_prefix} "
                f"(Brands {start_idx + 1}–{min(end_idx, total_rows)})"
            ),
        )
    )

    st.altair_chart(chart, use_container_width=True)

    # Pagination controls
    col_info, col_prev, col_page, col_next = st.columns([2, 1, 2, 1])

    with col_info:
        st.write("**Scroll to see more brands**")

    with col_prev:
        if st.button("⬅️ Previous", key=f"{session_key}_prev", use_container_width=True):
            if st.session_state[session_key] > 1:
                st.session_state[session_key] -= 1
                st.rerun()

    with col_page:
        st.markdown(
            f"<div style='text-align:center; font-size:16px; padding-top:8px;'>"
            f"Page {page} of {total_pages}"
            f"</div>",
            unsafe_allow_html=True,
        )

    with col_next:
        if st.button("Next ➡️", key=f"{session_key}_next", use_container_width=True):
            if st.session_state[session_key] < total_pages:
                st.session_state[session_key] += 1
                st.rerun()

# -----------------------------------------------------------------------------
# VIEW HANDLERS
# -----------------------------------------------------------------------------
def view_top_100_brands_overall():
    st.subheader("Top 100 Brands Overall")

    try:
        df = run_query(QUERIES["Top 100 Brands Overall"])
    except Exception as e:
        st.error(f"Database error: {e}")
        return

    if df.empty:
        st.warning("No data returned.")
        return

    # CHART AT TOP (using optimization_score, as before)
    show_brand_chart(
        df=df,
        metric_col="optimization_score",
        session_key="page_top100",
        title_prefix="Optimization Score",
        page_size=20,
    )

    # TABLE (ALL BRANDS)
    st.dataframe(df, use_container_width=True)


def view_default_table(view_name: str):
    st.subheader(view_name)

    try:
        df = run_query(QUERIES[view_name])
    except Exception as e:
        st.error(f"Database error: {e}")
        return

    if df.empty:
        st.warning("No data returned.")
        return

    # ---------- CHART AT TOP FOR EVERY OTHER VIEW ----------
    # Pick a reasonable metric column
    metric_col = None
    preferred_order = ["ranking_value", "ai_overall_score", "optimization_score"]

    for col in preferred_order:
        if col in df.columns:
            metric_col = col
            break

    if metric_col is not None and "brandname" in df.columns:
        # Nice label from the column name
        metric_label = metric_col.replace("_", " ").title()
        # Unique session key per view
        session_key = (
            "page_"
            + view_name.replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("/", "_")
            .lower()
        )

        show_brand_chart(
            df=df,
            metric_col=metric_col,
            session_key=session_key,
            title_prefix=metric_label,
            page_size=20,
        )

    # ---------- TABLE BELOW CHART ----------
    st.dataframe(df, use_container_width=True)

# -----------------------------------------------------------------------------
# SIDEBAR NAVIGATION + ROUTING
# -----------------------------------------------------------------------------
st.sidebar.title("Views")
view = st.sidebar.radio("Select a table", list(QUERIES.keys()))

if view == "Top 100 Brands Overall":
    view_top_100_brands_overall()
else:
    view_default_table(view)
