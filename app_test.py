# ===============================================
# F1 Tyre Strategy & Pit Stop Dashboard (FINAL FIX)
# ===============================================

import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt

st.set_page_config(page_title="F1 Tyre Strategy Dashboard", layout="wide")

# --------------------------------------------------
# Load Data (with robust merging)
# --------------------------------------------------
@st.cache_data
def load_data():
    conn = sqlite3.connect("f1_data.db")

    def safe_read(query):
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            st.warning(f"⚠️ Could not load table for query: {query[:30]}... — {e}")
            return pd.DataFrame()

    tyre_changes = safe_read("SELECT * FROM tyre_changes")
    stints = safe_read("SELECT * FROM stints")
    pitstops = safe_read("SELECT * FROM pitstops")
    drivers = safe_read("SELECT DISTINCT di.driver_id, full_name, team_name FROM  drivers_identity as di Join driver_sessions as ds ON ds.driver_id  = di.driver_id JOIN teams as t ON ds.team_id = t.team_id ORDER BY di.driver_id;")
    sessions = safe_read("SELECT DISTINCT session_key, circuit_short_name FROM circuits as c JOIN race_sessions as r ON c.circuit_key = r.circuit_key")


    conn.close()

    # Normalize column names
    if "driver" in tyre_changes.columns and "driver_number" not in tyre_changes.columns:
        tyre_changes.rename(columns={"driver": "driver_number"}, inplace=True)

    if "milliseconds" in pitstops.columns and "pit_duration" not in pitstops.columns:
        pitstops["pit_duration"] = pitstops["milliseconds"] / 1000.0

    # Merge team info into stints
    if "driver_number" in stints.columns and "driver_number" in drivers.columns:
        stints = stints.merge(
            drivers[["driver_number", "team_name"]],
            on="driver_number",
            how="left"
        )

    # Merge circuit info into stints
    if "session_key" in stints.columns and "session_key" in sessions.columns:
        stints = stints.merge(
            sessions[["session_key", "circuit_short_name"]],
            on="session_key",
            how="left"
        )

    # Merge into tyre_changes
    common_cols = [c for c in ["session_key", "driver_number"] if c in tyre_changes.columns and c in stints.columns]
    if common_cols:
        extra_cols = [c for c in ["team_name", "circuit_short_name"] if c in stints.columns]
        tyre_changes = tyre_changes.merge(
            stints[common_cols + extra_cols].drop_duplicates(),
            on=common_cols,
            how="left"
        )

    # Merge into pitstops
    common_cols = [c for c in ["session_key", "driver_number"] if c in pitstops.columns and c in stints.columns]
    if common_cols:
        extra_cols = [c for c in ["team_name", "circuit_short_name"] if c in stints.columns]
        pitstops = pitstops.merge(
            stints[common_cols + extra_cols].drop_duplicates(),
            on=common_cols,
            how="left"
        )

    # Minimal debug display
    st.sidebar.write("✅ `stints` columns:", stints.columns.tolist())

    return tyre_changes, stints, pitstops



# ✅ Load data
tyre_changes, stints, pitstops = load_data()

# --------------------------------------------------
# Sidebar Debug Info
# --------------------------------------------------
with st.sidebar.expander("✅ Loaded Data Tables"):
    st.write({
        "tyre_changes": list(tyre_changes.columns) if not tyre_changes.empty else "❌ empty",
        "stints": list(stints.columns) if not stints.empty else "❌ empty",
        "pitstops": list(pitstops.columns) if not pitstops.empty else "❌ empty",
    })

# --------------------------------------------------
# Sidebar Navigation
# --------------------------------------------------
st.sidebar.title("🏎️ Dashboard Navigation")
tabs = st.sidebar.radio(
    "Select Dashboard Section:",
    [
        "Tyre Change Frequency",
        "Position Change by Strategy",
        "Opening Tyre vs Δ Position",
        "Tyre Stint Map",
        "Pit Stop Insights",
        "Team Comparison"
    ]
)

# --------------------------------------------------
# Independent Filters per Visualization
# --------------------------------------------------
def apply_filters(df):
    sessions = sorted(df["session_key"].dropna().unique()) if "session_key" in df.columns else []
    teams = sorted(df["team_name"].dropna().unique()) if "team_name" in df.columns else []
    tracks = sorted(df["circuit_short_name"].dropna().unique()) if "circuit_short_name" in df.columns else []

    selected_session = st.selectbox("Select Session", ["All Sessions"] + list(map(str, sessions)))
    selected_team = st.selectbox("Select Team", ["All Teams"] + teams)
    selected_track = st.selectbox("Select Track", ["All Tracks"] + tracks)

    if selected_session != "All Sessions" and "session_key" in df.columns:
        df = df[df["session_key"].astype(str) == selected_session]
    if selected_team != "All Teams" and "team_name" in df.columns:
        df = df[df["team_name"] == selected_team]
    if selected_track != "All Tracks" and "circuit_short_name" in df.columns:
        df = df[df["circuit_short_name"] == selected_track]

    return df


# --------------------------------------------------
# TAB 1: Tyre Change Frequency
# --------------------------------------------------
if tabs == "Tyre Change Frequency":
    st.header("🏁 Tyre Change Frequency by Type")
    filtered = apply_filters(tyre_changes)

    if not filtered.empty and "change_type" in filtered.columns:
        freq = (
            filtered.groupby("change_type")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )

        fig = px.bar(
            freq,
            x="count",
            y="change_type",
            orientation="h",
            title="Frequency of Tyre Compound Changes",
            color="change_type",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No tyre change data available to plot.")


# --------------------------------------------------
# TAB 2: Position Change by Strategy
# --------------------------------------------------
elif tabs == "Position Change by Strategy":
    st.header("📊 Position Change by Strategy Archetype")
    filtered = apply_filters(tyre_changes)

    if not filtered.empty and {"change_type", "position_change"} <= set(filtered.columns):
        show_winners = st.checkbox("Show Winners Only", value=False)
        if show_winners and "pos_after" in filtered.columns:
            filtered = filtered[filtered["pos_after"] == 1]

        plt.figure(figsize=(14, 7))
        sns.boxplot(data=filtered, x="change_type", y="position_change", palette="Set2")
        plt.title("Δ Position by Strategy Archetype")
        plt.ylabel("Δ Position (Final - Grid)")
        plt.xlabel("Strategy Archetype")
        plt.xticks(rotation=30)
        st.pyplot(plt)
    else:
        st.warning("Missing required columns for this visualization.")


# --------------------------------------------------
# TAB 3: Opening Tyre vs Δ Position
# --------------------------------------------------
# --------------------------------------------------
# TAB 3: Opening Tyre vs Δ Position (FIXED)
# --------------------------------------------------
elif tabs == "Opening Tyre vs Δ Position":
    st.header("🚦 Opening Tyre Choice vs Δ Position")

    # Use stints instead of tyre_changes
    filtered = apply_filters(stints)

    if not filtered.empty and "compound" in filtered.columns:
        # Keep only the first stint per driver (opening tyre)
        opening_stints = (
            filtered.sort_values("stint_number")
            .groupby(["session_key", "driver_number"], as_index=False)
            .first()
        )

        # Merge Δ position from tyre_changes (if columns exist)
        if not tyre_changes.empty and {"driver_number", "position_change"} <= set(tyre_changes.columns):
            merged = pd.merge(
                opening_stints,
                tyre_changes[["driver_number", "session_key", "position_change"]],
                on=["driver_number", "session_key"],
                how="left"
            )
        else:
            merged = opening_stints.copy()
            merged["position_change"] = None

        # Filter to standard dry compounds
        merged = merged[merged["compound"].isin(["SOFT", "MEDIUM", "HARD"])]

        if merged["position_change"].notna().any():
            plt.figure(figsize=(8, 5))
            sns.boxplot(data=merged, x="compound", y="position_change", palette="coolwarm")
            plt.title("Opening Tyre Choice vs Δ Position")
            plt.xlabel("Opening Tyre")
            plt.ylabel("Δ Position (Final - Grid)")
            st.pyplot(plt)
        else:
            st.info("No Δ position data found for selected filters.")
    else:
        st.warning("`compound` column not found in the stints dataset.")


# --------------------------------------------------
# TAB 4: Tyre Stint Map
# --------------------------------------------------
elif tabs == "Tyre Stint Map":
    st.header("🗺️ Tyre Stint Map by Driver")

    if not stints.empty and "session_key" in stints.columns:
        sessions = sorted(stints["session_key"].dropna().unique())
        selected_session = st.selectbox("Select Session", sessions)
        stints_filtered = stints[stints["session_key"] == selected_session]

        plt.figure(figsize=(14, 8))
        stints_sorted = stints_filtered.sort_values(["driver_number", "stint_number"])
        colors = {"SOFT": "red", "MEDIUM": "yellow", "HARD": "gray"}

        drivers = sorted(stints_sorted["driver_number"].unique())
        for i, driver in enumerate(drivers):
            drv_stints = stints_sorted[stints_sorted.driver_number == driver]
            for _, row in drv_stints.iterrows():
                plt.hlines(
                    y=i,
                    xmin=row.lap_start,
                    xmax=row.lap_end,
                    color=colors.get(row.compound, "black"),
                    linewidth=10,
                )

        plt.yticks(range(len(drivers)), drivers)
        plt.xlabel("Lap Number")
        plt.ylabel("Driver Number")
        plt.title(f"Tyre Stint Map (Session {selected_session})")
        st.pyplot(plt)
    else:
        st.warning("No stint data found.")


# --------------------------------------------------
# TAB 5: Pit Stop Insights
# --------------------------------------------------
elif tabs == "Pit Stop Insights":
    st.header("⏱️ Pit Stop Insights")

    if not pitstops.empty:
        teams = sorted(pitstops["team_name"].dropna().unique()) if "team_name" in pitstops.columns else []
        selected_team = st.selectbox("Select Team", ["All Teams"] + teams)

        filtered = pitstops.copy()
        if selected_team != "All Teams" and "team_name" in filtered.columns:
            filtered = filtered[filtered["team_name"] == selected_team]

        show_winner_only = st.checkbox("Show Winners Only", value=False)
        if show_winner_only and "position" in filtered.columns:
            filtered = filtered[filtered["position"] == 1]

        if "pit_duration" in filtered.columns:
            fig = px.box(
                filtered,
                x="team_name",
                y="pit_duration",
                color="team_name",
                title="Pit Stop Duration by Team",
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("`pit_duration` column not found in pitstops table.")
    else:
        st.warning("Pit stop data unavailable.")


# --------------------------------------------------
# TAB 6: Team Comparison
# --------------------------------------------------
elif tabs == "Team Comparison":
    st.header("⚔️ Average Position Change Comparison Between Teams")
    filtered = apply_filters(tyre_changes)

    if not filtered.empty and "team_name" in filtered.columns:
        teams = sorted(filtered["team_name"].dropna().unique())
        if len(teams) >= 2:
            team1 = st.selectbox("Select First Team", teams, index=0)
            team2 = st.selectbox("Select Second Team", [t for t in teams if t != team1], index=1)
            df_compare = filtered[filtered["team_name"].isin([team1, team2])]

            avg_change = (
                df_compare.groupby(["team_name", "change_type"])["position_change"]
                .mean()
                .reset_index()
            )

            fig = px.bar(
                avg_change,
                x="change_type",
                y="position_change",
                color="team_name",
                barmode="group",
                title=f"Average Position Change by Tyre Compound ({team1} vs {team2})",
                labels={"position_change": "Avg Δ Position", "change_type": "Tyre Change"},
                color_discrete_sequence=px.colors.qualitative.Vivid,
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Need at least 2 teams in dataset to compare.")
    else:
        st.warning("No team data found in dataset.")


# --------------------------------------------------
# Footer
# --------------------------------------------------
st.markdown("---")
st.caption("Developed for MLDS F1 Tyre Strategy Project • Data from openf1.org API")
