import streamlit as st
import pandas as pd
import plotly.express as px
from trino.dbapi import connect as trino_connect
import os

st.set_page_config(layout="wide", page_title="Flight Delays Dashboard")

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = os.environ.get("TRINO_PORT", 8081)
TRINO_USER = os.environ.get("TRINO_USER", "admin")
TRINO_PASS = os.environ.get("TRINO_PASS", "")


def get_conn():
    return trino_connect(
        host=TRINO_HOST,
        port=int(TRINO_PORT),
        user=TRINO_USER,
        catalog="delta",
        schema="gold",
    )


def run_query(sql):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=cols)


@st.cache_data(ttl=600)
def load_airlines():
    return run_query(
        "SELECT iata_code, airline FROM delta.bronze.airlines ORDER BY airline"
    )


@st.cache_data(ttl=600)
def load_airports():
    return run_query(
        "SELECT iata_code, airport, city, state "
        "FROM delta.bronze.airports ORDER BY airport"
    )


@st.cache_data(ttl=600)
def load_kpis():
    sql = """
    SELECT
        COUNT(*)                                         AS total_flights,
        ROUND(AVG(arrival_delay), 2)                     AS avg_arrival_delay,
        ROUND(AVG(departure_delay), 2)                   AS avg_departure_delay,
        ROUND(SUM(CASE WHEN arrival_delay > 15 THEN 1 ELSE 0 END)
            * 100.0 / NULLIF(COUNT(*), 0), 2)            AS delay_pct,
        ROUND(SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END)
            * 100.0 / NULLIF(COUNT(*), 0), 2)            AS cancel_pct
    FROM fact_flights_delay
    """
    return run_query(sql)


@st.cache_data(ttl=600)
def load_worst_airline():
    sql = """
    SELECT a.airline
    FROM fact_flights_delay f
    JOIN delta.bronze.airlines a ON f.airline_code = a.iata_code
    GROUP BY a.airline
    ORDER BY AVG(f.arrival_delay) DESC
    LIMIT 1
    """
    df = run_query(sql)
    return df.iloc[0, 0] if not df.empty else "N/A"


@st.cache_data(ttl=600)
def load_top_airlines_data():
    sql = """
    SELECT a.airline,
           COUNT(*)                                        AS num_flights,
           ROUND(AVG(f.arrival_delay), 2)                  AS avg_arrival_delay,
           ROUND(SUM(CASE WHEN f.arrival_delay > 15 THEN 1 ELSE 0 END)
               * 100.0 / NULLIF(COUNT(*), 0), 2)           AS delay_pct
    FROM fact_flights_delay f
    JOIN delta.bronze.airlines a ON f.airline_code = a.iata_code
    GROUP BY a.airline
    ORDER BY avg_arrival_delay DESC
    """
    return run_query(sql)


@st.cache_data(ttl=600)
def load_trend_filtered(airline_codes_tuple, airport_codes_tuple, date_from, date_to):
    clauses = ["TRUE"]
    if airline_codes_tuple:
        items = ",".join(f"'{c}'" for c in airline_codes_tuple)
        clauses.append(f"airline_code IN ({items})")
    if airport_codes_tuple:
        items = ",".join(f"'{c}'" for c in airport_codes_tuple)
        clauses.append(f"origin_airport IN ({items})")
    if date_from:
        clauses.append(f"flight_date >= DATE '{date_from}'")
    if date_to:
        clauses.append(f"flight_date <= DATE '{date_to}'")
    where = " AND ".join(clauses)
    sql = f"""
    SELECT date_trunc('month', flight_date) AS period,
           COUNT(*)                                          AS num_flights,
           ROUND(AVG(arrival_delay), 2)                      AS avg_arrival_delay,
           ROUND(AVG(departure_delay), 2)                    AS avg_departure_delay
    FROM fact_flights_delay
    WHERE {where}
    GROUP BY 1
    ORDER BY 1
    """
    return run_query(sql)


@st.cache_data(ttl=600)
def load_delay_reasons():
    sql = """
    SELECT 'Airline Delay'                          AS reason,
           ROUND(SUM(COALESCE(airline_delay,0)), 2)  AS total_minutes
    FROM delta.bronze.flights
    UNION ALL
    SELECT 'Weather Delay',
           ROUND(SUM(COALESCE(weather_delay,0)), 2)
    FROM delta.bronze.flights
    UNION ALL
    SELECT 'NAS Delay',
           ROUND(SUM(COALESCE(air_system_delay,0)), 2)
    FROM delta.bronze.flights
    UNION ALL
    SELECT 'Late Aircraft Delay',
           ROUND(SUM(COALESCE(late_aircraft_delay,0)), 2)
    FROM delta.bronze.flights
    UNION ALL
    SELECT 'Security Delay',
           ROUND(SUM(COALESCE(security_delay,0)), 2)
    FROM delta.bronze.flights
    """
    return run_query(sql)


@st.cache_data(ttl=600)
def load_top_airports_data():
    sql = """
    SELECT ap.airport, ap.city, ap.state,
           COUNT(*)                                         AS num_departures,
           ROUND(AVG(f.arrival_delay), 2)                   AS avg_arrival_delay,
           ROUND(SUM(CASE WHEN f.arrival_delay > 15 THEN 1 ELSE 0 END)
               * 100.0 / NULLIF(COUNT(*), 0), 2)            AS delay_pct
    FROM fact_flights_delay f
    JOIN delta.bronze.airports ap ON f.origin_airport = ap.iata_code
    GROUP BY ap.airport, ap.city, ap.state
    ORDER BY delay_pct DESC
    """
    return run_query(sql)


def main():
    st.title("Flight Delays Analysis Dashboard")
    st.markdown("""
    *Medallion Lakehouse — Bronze → Silver → Gold | Trino + Delta Lake on MinIO*
    """)

    airlines_df = load_airlines()
    airports_df = load_airports()

    with st.sidebar:
        st.header("Filters")

        airline_names = st.multiselect(
            "Airline",
            options=airlines_df["airline"].tolist(),
            placeholder="All airlines",
        )
        airport_names = st.multiselect(
            "Origin Airport",
            options=airports_df["airport"].tolist(),
            placeholder="All airports",
        )

        st.divider()
        min_date = run_query(
            "SELECT MIN(flight_date) FROM fact_flights_delay"
        ).iloc[0, 0]
        max_date = run_query(
            "SELECT MAX(flight_date) FROM fact_flights_delay"
        ).iloc[0, 0]
        date_range = st.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

    airline_codes = tuple(
        airlines_df[airlines_df["airline"].isin(airline_names)]["iata_code"]
    ) if airline_names else ()
    airport_codes = tuple(
        airports_df[airports_df["airport"].isin(airport_names)]["iata_code"]
    ) if airport_names else ()
    dr = date_range if isinstance(date_range, (list, tuple)) else (date_range, None)
    date_from = str(dr[0]) if len(dr) > 0 else ""
    date_to = str(dr[1]) if len(dr) > 1 and dr[1] else ""

    kpi = load_kpis()
    worst = load_worst_airline()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Flights", f"{kpi.iloc[0]['total_flights']:,}")
    with col2:
        st.metric("Avg Delay", f"{kpi.iloc[0]['avg_arrival_delay']} min")
    with col3:
        on_time = 100 - kpi.iloc[0]["delay_pct"]
        st.metric("On-Time Rate", f"{on_time:.2f}%")
    with col4:
        st.metric("Worst Airline", worst)

    tab1, tab2, tab3, tab4 = st.tabs([
        "Airlines", "Trends", "Delay Reasons", "Airports"
    ])

    with tab1:
        df_top = load_top_airlines_data()
        fig = px.bar(
            df_top.head(10),
            x="avg_arrival_delay",
            y="airline",
            orientation="h",
            color="avg_arrival_delay",
            color_continuous_scale="RdYlGn_r",
            text=df_top.head(10)["avg_arrival_delay"]
            .apply(lambda v: f"{v:.1f} min"),
            labels={
                "airline": "",
                "avg_arrival_delay": "Avg Arrival Delay (min)",
            },
            title="Top 10 Airlines by Average Arrival Delay",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(height=450)
        st.plotly_chart(fig, width='stretch')

        with st.expander("Full Data"):
            st.dataframe(
                df_top.style.format({
                    "avg_arrival_delay": "{:.2f}",
                    "delay_pct": "{:.2f}%",
                    "num_flights": "{:,}",
                }),
                width='stretch',
                hide_index=True,
            )

    with tab2:
        st.subheader("Arrival Delay Trend (Monthly)")
        df_trend = load_trend_filtered(
            airline_codes, airport_codes, date_from, date_to
        )
        fig = px.line(
            df_trend,
            x="period",
            y="avg_arrival_delay",
            markers=True,
            labels={"period": "", "avg_arrival_delay": "Avg Delay (min)"},
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, width='stretch')

    with tab3:
        df_reasons = load_delay_reasons()
        fig = px.pie(
            df_reasons,
            values="total_minutes",
            names="reason",
            hole=0.4,
            title="Delay Causes Breakdown",
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        fig.update_traces(
            textposition="inside",
            textinfo="percent+label",
            hovertemplate=(
                "<b>%{label}</b><br>%{value:,.0f} min<br>%{percent}"
            ),
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, width='stretch')

    with tab4:
        df_airports = load_top_airports_data()
        df_top_ap = df_airports.head(10).copy()
        df_top_ap["label"] = (
            df_top_ap["airport"].str[:35]
            + " (" + df_top_ap["state"] + ")"
        )
        fig = px.bar(
            df_top_ap,
            x="delay_pct",
            y="label",
            orientation="h",
            color="delay_pct",
            color_continuous_scale="RdYlGn_r",
            text=df_top_ap["delay_pct"].apply(lambda v: f"{v:.1f}%"),
            labels={"label": "", "delay_pct": "Delay Rate (%)"},
            title="Top 10 Airports by Delay Rate (arrival > 15 min)",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(height=450)
        st.plotly_chart(fig, width='stretch')


if __name__ == "__main__":
    main()
