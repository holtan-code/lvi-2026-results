import math

import pandas as pd
import streamlit as st
import altair as alt
from sqlalchemy import create_engine, text

# -----------------------------------------------------------------------------
# PAGE CONFIG
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="2026 LVI Results",
    layout="wide",
)

st.title("2026 LVI Results")

# -----------------------------------------------------------------------------
# DIRECT DB CONNECTION
# -----------------------------------------------------------------------------
# (If you want, you can swap this to read from an env var:
#   import os
#   db_url = os.getenv("DATABASE_URL")
# )
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
        WITH top_ranks AS (
            SELECT brand_id, rank
            FROM lvi_2026_ranking_ch
            ORDER BY rank
            LIMIT 100
        )
        SELECT
            tr.rank AS rank,
            s.*,
            p.platform_a,
            p.platform_b
        FROM top_ranks tr
        LEFT JOIN lvi_2026_brand_scores_ch s
          ON tr.brand_id = s.brand_id
        LEFT JOIN lvi_2024_brand_national_location_count m
          ON tr.brand_id = m.brandid
        LEFT JOIN "VOID-lvi_2026_brand_platform_list-12-18-2025" p
          ON tr.brand_id = p.brandid
        ORDER BY tr.rank;
    """,

    "Top 100 Enterprise Brands": """
        WITH base AS (
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
        ),
        ranked AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY ranking_value DESC) AS rank,
                *
            FROM base
        )
        SELECT *
        FROM ranked
        WHERE rank <= 100
        ORDER BY rank;
    """,

    "Top 100 Enterprise AI Brands": """
        WITH base AS (
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
                s.social_score,
                s.ranking_value
            FROM lvi_2026_brand_scores_ch s
            JOIN lvi_2024_brand_national_location_count m
              ON s.brand_id = m.brandid
            LEFT JOIN lvi_2026_brand_platform_list p
              ON m.brandid = p.brandid
            WHERE m.total_us_locations >= 500
        ),
        ranked AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY ranking_value DESC) AS rank,
                *
            FROM base
        )
        SELECT *
        FROM ranked
        WHERE rank <= 100
        ORDER BY rank;
    """,

    "Top 100 Enterprise Food Brands": """
        WITH base AS (
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
        ),
        ranked AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY ranking_value DESC) AS rank,
                *
            FROM base
        )
        SELECT *
        FROM ranked
        WHERE rank <= 100
        ORDER BY rank;
    """,

    "Top 100 Enterprise Retail Brands": """
        WITH base AS (
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
        ),
        ranked AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY ranking_value DESC) AS rank,
                *
            FROM base
        )
        SELECT *
        FROM ranked
        WHERE rank <= 100
        ORDER BY rank;
    """,

    "Top 100 Enterprise Financial Services Brands": """
        WITH base AS (
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
        ),
        ranked AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY ranking_value DESC) AS rank,
                *
            FROM base
        )
        SELECT *
        FROM ranked
        WHERE rank <= 100
        ORDER BY rank;
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
# TABLE HELPER (HIDE INDEX)
# -----------------------------------------------------------------------------
def show_table(df: pd.DataFrame):
    """Show dataframe without index column if possible."""
    try:
        st.dataframe(df, use_container_width=True, hide_index=True)
    except TypeError:
        st.dataframe(df.reset_index(drop=True), use_container_width=True)

# -----------------------------------------------------------------------------
# GENERIC METRIC CHART HELPER (TOP-OF-VIEW CHART)
# -----------------------------------------------------------------------------
def show_brand_chart(
    df: pd.DataFrame,
    metric_col: str,
    session_key: str,
    title_prefix: str,
    page_size: int = 20,
):
    """Paginated bar chart of metric_col by brandname."""
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
# PLATFORM COUNT + SCATTER ROW (BELOW TABLE)
# -----------------------------------------------------------------------------
def show_platform_and_scatter_row(df: pd.DataFrame, view_name: str):
    """
    Below-table visuals:
      - Left: vertical bar chart of platform counts (platform_a + platform_b)
      - Right: scatter plot (x = optimization_score, y = ai_overall_score),
               color-coded by platform_a (NULL/empty -> 'N/A'),
               with:
                 * axes domains starting near min values (with padding)
                 * legend under the chart (no title)
                 * interactive brushing
    """
    col_left, col_right = st.columns(2)

    # LEFT: PLATFORM COUNT BAR CHART
    with col_left:
        if "platform_a" not in df.columns and "platform_b" not in df.columns:
            st.info("No platform columns (platform_a / platform_b) available for chart.")
        else:
            series_list = []
            if "platform_a" in df.columns:
                series_list.append(df["platform_a"])
            if "platform_b" in df.columns:
                series_list.append(df["platform_b"])

            if series_list:
                platform_values = pd.concat(series_list, ignore_index=True).dropna()
                platform_values = platform_values.astype(str).str.strip()
                platform_values = platform_values[platform_values != ""]

                if platform_values.empty:
                    st.info("No platform data available.")
                else:
                    counts = platform_values.value_counts().reset_index()
                    counts.columns = ["platform", "count"]

                    platform_chart = (
                        alt.Chart(counts)
                        .mark_bar()
                        .encode(
                            x=alt.X(
                                "platform:N",
                                title="Platform",
                                sort="-y",  # order by count desc
                            ),
                            y=alt.Y("count:Q", title="Count"),
                            tooltip=[
                                alt.Tooltip("platform:N", title="Platform"),
                                alt.Tooltip("count:Q", title="Count"),
                            ],
                        )
                        .properties(
                            title=f"Platform Counts ({view_name})",
                            height=400,
                            width="container",
                        )
                    )

                    st.altair_chart(platform_chart, use_container_width=True)
            else:
                st.info("No platform data available.")

    # RIGHT: SCATTER PLOT
    with col_right:
        required_cols = {"optimization_score", "ai_overall_score"}
        if not required_cols.issubset(df.columns):
            st.info(
                "Scatter plot requires 'optimization_score' and 'ai_overall_score' columns."
            )
        else:
            scatter_df = df.copy()

            # Color label: platform_a, with NULL/empty -> 'N/A'
            if "platform_a" in scatter_df.columns:
                scatter_df["platform_a_label"] = (
                    scatter_df["platform_a"]
                    .fillna("N/A")
                    .astype(str)
                    .str.strip()
                    .replace("", "N/A")
                )
            else:
                scatter_df["platform_a_label"] = "N/A"

            # Drop rows missing x or y
            scatter_df = scatter_df.dropna(
                subset=["optimization_score", "ai_overall_score"]
            )

            if scatter_df.empty:
                st.info("No data available for scatter plot.")
            else:
                # Axis domains with padding
                x_min = scatter_df["optimization_score"].min()
                x_max = scatter_df["optimization_score"].max()
                y_min = scatter_df["ai_overall_score"].min()
                y_max = scatter_df["ai_overall_score"].max()

                if x_min == x_max:
                    x_min -= 1
                    x_max += 1
                if y_min == y_max:
                    y_min -= 1
                    y_max += 1

                x_padding = (x_max - x_min) * 0.05
                y_padding = (y_max - y_min) * 0.05
                x_domain = [x_min - x_padding, x_max + x_padding]
                y_domain = [y_min - y_padding, y_max + y_padding]

                brush = alt.selection_interval()

                scatter_chart = (
                    alt.Chart(scatter_df)
                    .mark_circle(size=60)
                    .encode(
                        x=alt.X(
                            "optimization_score:Q",
                            title="Optimization Score",
                            scale=alt.Scale(domain=x_domain),
                        ),
                        y=alt.Y(
                            "ai_overall_score:Q",
                            title="AI Overall Score",
                            scale=alt.Scale(domain=y_domain),
                        ),
                        color=alt.Color(
                            "platform_a_label:N",
                            legend=alt.Legend(orient="bottom", title=None),
                        ),
                        opacity=alt.condition(brush, alt.value(1.0), alt.value(0.3)),
                        tooltip=[
                            alt.Tooltip("brandname:N", title="Brand"),
                            alt.Tooltip("optimization_score:Q", title="Optimization"),
                            alt.Tooltip("ai_overall_score:Q", title="AI Overall"),
                            alt.Tooltip("platform_a_label:N", title="Platform A"),
                        ],
                    )
                    .add_selection(brush)
                    .properties(
                        title=f"AI vs Optimization by Platform ({view_name})",
                        height=400,
                        width="container",
                    )
                )

                st.altair_chart(scatter_chart, use_container_width=True)

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

    # METRIC CHART AT TOP (Optimization Score if present)
    if "optimization_score" in df.columns:
        show_brand_chart(
            df=df,
            metric_col="optimization_score",
            session_key="page_top100",
            title_prefix="Optimization Score",
            page_size=20,
        )

    # TABLE (NO INDEX)
    show_table(df)

    # PLATFORM COUNT + SCATTER ROW
    show_platform_and_scatter_row(df, "Top 100 Brands Overall")


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

    # METRIC CHART AT TOP (best available metric)
    metric_col = None
    preferred_order = ["ranking_value", "ai_overall_score", "optimization_score"]

    for col in preferred_order:
        if col in df.columns:
            metric_col = col
            break

    if metric_col is not None and "brandname" in df.columns:
        metric_label = metric_col.replace("_", " ").title()
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

    # TABLE (NO INDEX)
    show_table(df)

    # PLATFORM COUNT + SCATTER ROW
    show_platform_and_scatter_row(df, view_name)

# -----------------------------------------------------------------------------
# SIDEBAR NAVIGATION + ROUTING
# -----------------------------------------------------------------------------
st.sidebar.title("Views")
view = st.sidebar.radio(
    "Select a table",
    list(QUERIES.keys()),
)

if view == "Top 100 Brands Overall":
    view_top_100_brands_overall()
else:
    view_default_table(view)
