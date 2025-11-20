import requests
import time
import pandas as pd
import sqlite3
from tqdm import tqdm

BASE = "https://api.openf1.org/v1"

# -----------------------------
# Fetch Utility
# -----------------------------
def fetch(endpoint, params=None, max_retries=5, base_url=BASE):
    for attempt in range(max_retries):
        try:
            r = requests.get(f"{base_url}/{endpoint}", params=params)

            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue

            r.raise_for_status()
            return pd.DataFrame(r.json())

        except Exception as e:
            wait = 2 ** attempt
            print(f"Error fetching {endpoint} {params}: {e} → retrying in {wait}s")
            time.sleep(wait)

    raise Exception(f"❌ Failed to fetch {endpoint} after retries.")

# -----------------------------
# EXTRACT
# -----------------------------
def get_race_sessions(start_year=2023):
    """
    Returns:
        filtered_meetings: meetings >= start_year
        race_sessions: only Race sessions for those meetings
        all_sessions: full sessions table (needed later)
    """
    meetings = fetch("meetings")
    filtered_meetings = meetings[meetings["year"] >= start_year].reset_index(drop=True)

    meeting_keys = filtered_meetings["meeting_key"].unique()

    sessions = fetch("sessions")
    race_sessions = sessions[
        (sessions["meeting_key"].isin(meeting_keys)) &
        (sessions["session_name"] == "Race")
    ].reset_index(drop=True)

    print("Meetings:", filtered_meetings.shape)
    print("Race Sessions:", race_sessions.shape)

    return filtered_meetings, sessions, race_sessions

def get_qual_sessions(filtered_meetings, all_sessions):
    """
    Returns all qualifying sessions for the selected meetings.
    """
    meeting_keys = filtered_meetings["meeting_key"].unique()

    qual_sessions = all_sessions[
        (all_sessions["meeting_key"].isin(meeting_keys)) &
        (all_sessions["session_name"] == "Qualifying")
    ].reset_index(drop=True)

    print("Qualifying Sessions:", qual_sessions.shape)
    return qual_sessions

def extract_data(start_year=2023, sleep_sec=0.15):
    """
    Extracts:
       - race sessions
       - qual sessions
       - all race-related datasets (results, pit, stints, laps, rc, weather, position)
       - all grid datasets (from race + qual)
       - driver metadata
    """
    # --- STEP 1: get race sessions ---
    filtered_meetings, sessions, race_sessions = get_race_sessions(start_year)
    race_session_keys = race_sessions["session_key"].unique()

    # --- STEP 2: get qualifying sessions ---
    qual_sessions = get_qual_sessions(filtered_meetings, sessions)
    qual_session_keys = qual_sessions["session_key"].unique()

    # Prepare collectors
    all_results, all_pit, all_stints, all_laps = [], [], [], []
    all_rc, all_weather = [], []
    all_position, all_grid = [], []

    # --- STEP 3: fetch race session data ---
    print(f"\nLoading race data for {len(race_session_keys)} sessions...")
    for sk in tqdm(race_session_keys, desc="Race sessions"):
        time.sleep(sleep_sec)

        for endpoint, collector in [
            ("session_result", all_results),
            ("pit", all_pit),
            ("stints", all_stints),
            ("laps", all_laps),
            ("race_control", all_rc),
            ("weather", all_weather),
            ("position", all_position),
            ("starting_grid", all_grid),
        ]:
            df = fetch(endpoint, {"session_key": sk})
            if not df.empty:
                df["session_key"] = sk
                collector.append(df)

    # --- STEP 4: fetch qualifying grid data ---
    print(f"\nLoading grid from {len(qual_session_keys)} quali sessions...")
    for sk in tqdm(qual_session_keys, desc="Quali sessions"):
        time.sleep(sleep_sec)
        df = fetch("starting_grid", {"session_key": sk})
        if not df.empty:
            df["session_key"] = sk
            all_grid.append(df)

    # --- STEP 5: fix lap date_start ---
    for l in all_laps:
        l["date_start"] = pd.to_datetime(l["date_start"], utc=True, errors="coerce")
        min_ts = l["date_start"].dropna().min()
        l.loc[
            (l["lap_number"] == 1) & (l["date_start"].isna()),
            "date_start"
        ] = min_ts

    # --- STEP 6: combine ---
    results = pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()
    pitstops = pd.concat(all_pit, ignore_index=True) if all_pit else pd.DataFrame()
    stints = pd.concat(all_stints, ignore_index=True) if all_stints else pd.DataFrame()
    laps = pd.concat(all_laps, ignore_index=True) if all_laps else pd.DataFrame()
    race_control = pd.concat(all_rc, ignore_index=True) if all_rc else pd.DataFrame()
    weather = pd.concat(all_weather, ignore_index=True) if all_weather else pd.DataFrame()
    grid = pd.concat(all_grid, ignore_index=True) if all_grid else pd.DataFrame()
    position = pd.concat(all_position, ignore_index=True) if all_position else pd.DataFrame()

    # Drivers metadata (race sessions only)
    drivers = fetch("drivers")
    drivers = drivers[drivers["session_key"].isin(race_session_keys)].reset_index(drop=True)

    print("\nExtract Summary:")
    print("Results:", results.shape)
    print("Pit Stops:", pitstops.shape)
    print("Stints:", stints.shape)
    print("Laps:", laps.shape)
    print("Race Control:", race_control.shape)
    print("Weather:", weather.shape)
    print("Grid:", grid.shape)
    print("Position:", position.shape)
    print("Drivers:", drivers.shape)

    return {
        "filtered_meetings": filtered_meetings,
        "sessions": sessions,
        "race_sessions": race_sessions,
        "qual_sessions": qual_sessions,
        "results": results,
        "pitstops": pitstops,
        "stints": stints,
        "laps": laps,
        "race_control": race_control,
        "weather": weather,
        "grid": grid,
        "position": position,
        "drivers": drivers,
    }

# -----------------------------
# TRANSFORM
# -----------------------------

def transform_data(raw):
    """
    Transform raw DataFrames into clean, relational tables.
    Returns a dict of cleaned/transformed DataFrames ready to load.
    """
    race_sessions = raw["race_sessions"]
    results = raw["results"].copy()
    pitstops = raw["pitstops"].copy()
    stints = raw["stints"].copy()
    laps = raw["laps"].copy()
    race_control = raw["race_control"].copy()
    weather = raw["weather"].copy()
    grid = raw["grid"].copy()
    position = raw["position"].copy()
    drivers = raw["drivers"].copy()

    # [1] Circuits
    circuits = (
        race_sessions [["circuit_key", "country_code", "circuit_short_name"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # [2] Race sessions
    race_sessions_clean = race_sessions[[
        "session_key", "circuit_key", "date_start", "date_end", "year"
    ]].copy()

    # [3] Teams & team_seasons using TEAM_LINEAGE
    TEAM_LINEAGE = {
        "Red Bull Racing": 1,
        "Mercedes": 2,
        "Ferrari": 3,
        "McLaren": 4,
        "Alpine": 5,
        "Aston Martin": 6,
        "Williams": 7,
        "Haas F1 Team": 8,
        # AlphaTauri lineage
        "AlphaTauri": 9,
        "RB": 9,
        "Racing Bulls": 9,
        # Sauber lineage
        "Alfa Romeo": 10,
        "Kick Sauber": 10,
    }

    teams = drivers.merge(
        race_sessions[["session_key", "year"]],
        on="session_key",
        how="left"
    )[["team_name", "team_colour", "year"]].copy()

    teams["team_colour"] = teams["team_colour"].str.lower()
    teams = teams.drop_duplicates().reset_index(drop=True)

    lineage_df = (
        pd.DataFrame.from_dict(TEAM_LINEAGE, orient="index", columns=["team_id"])
        .reset_index()
        .rename(columns={"index": "team_name"})
    )

    teams_with_id = teams.merge(lineage_df, on="team_name", how="left")

    # Keep latest name/colour per team_id
    teams_clean = (
        teams_with_id
        .sort_values(["team_id", "year"])
        .groupby("team_id")
        .tail(1)
        [["team_id", "team_name", "team_colour"]]
        .sort_values("team_id")
        .reset_index(drop=True)
    )

    # Historic
    team_seasons = (
        teams_with_id[["team_id", "team_name", "year", "team_colour"]]
        .drop_duplicates()
        .sort_values(["team_id", "year"])
        .reset_index(drop=True)
    )

    # [4] Drivers & driver_sessions
    drivers_merge = drivers.merge(
        team_seasons[["team_id", "team_name", "year", "team_colour"]],
        on=["team_name"],
        how="left",
        suffixes=("", "_season")
    )

    drivers_clean = (
        drivers_merge[[
            "driver_number",
            "broadcast_name",
            "full_name",
            "name_acronym",
            "team_id",
            "year"
        ]]
        .drop_duplicates()
        .sort_values(["driver_number", "team_id", "year"])
        .reset_index(drop=True)
    )

    # Normalize names
    for col in ["full_name", "broadcast_name", "name_acronym"]:
        drivers_clean[col] = drivers_clean[col].astype(str).str.strip()

    drivers_identity = (
        drivers_clean[["full_name", "broadcast_name", "name_acronym"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    drivers_identity["driver_id"] = drivers_identity.index + 1

    driver_sessions = drivers_merge.merge(
        drivers_identity,
        on=["full_name", "broadcast_name", "name_acronym"],
        how="left"
    )[["driver_number", "session_key", "team_id", "driver_id"]] \
        .drop_duplicates().reset_index(drop=True)

    # [5] Weather
    if "meeting_key" in weather.columns:
        weather_clean = weather.drop(columns=["meeting_key"])
    else:
        weather_clean = weather

    # [6] Race control
    if "meeting_key" in race_control.columns:
        race_control_clean = race_control.drop(columns=["meeting_key"])
    else:
        race_control_clean = race_control

    # [7] Results
    results["position"] = pd.to_numeric(results["position"], errors="coerce").astype("Int64")
    results["number_of_laps"] = pd.to_numeric(results["number_of_laps"], errors="coerce").astype("Int64")
    results["points"] = pd.to_numeric(results["points"], errors="coerce").astype("Int64")

    # status: finish/dnf/dns/dsq
    results["status"] = results["dnf"].apply(lambda x: "dnf" if x else "finish")
    results.loc[results["dns"] == True, "status"] = "dns"
    results.loc[results["dsq"] == True, "status"] = "dsq"

    results_clean = results[[
        "session_key",
        "position",
        "driver_number",
        "number_of_laps",
        "points",
        "duration",
        "gap_to_leader",
        "dnf",
        "dns",
        "dsq"
    ]].copy()

    # [8] Pitstops
    if "meeting_key" in pitstops.columns:
        pitstops_clean = pitstops.drop(columns=["meeting_key"])
    else:
        pitstops_clean = pitstops

    # [9] Stints
    if "meeting_key" in stints.columns:
        stints_clean = stints.drop(columns=["meeting_key"])
    else:
        stints_clean = stints

    # [10] Laps
    if "meeting_key" in laps.columns:
        laps_clean = laps.drop(columns=["meeting_key"])
    else:
        laps_clean = laps

    # [11] Position
    if "meeting_key" in position.columns:
        position_clean = position.drop(columns=["meeting_key"])
    else:
        position_clean = position

    # [12] Grid → map to race session_key
    race_map = (
        race_sessions[
            race_sessions["session_name"].str.lower() == "race"
        ][["meeting_key", "session_key"]]
        .rename(columns={"session_key": "race_session_key"})
        .drop_duplicates("meeting_key")
    )

    if "meeting_key" in grid.columns:
        grid_with_race = grid.merge(race_map, on="meeting_key", how="left")
    else:
        # If meeting_key is missing somehow, just keep as-is (fallback)
        grid_with_race = grid.copy()
        grid_with_race = grid_with_race.merge(race_map, on="session_key", how="left")

    grid_with_race["session_key"] = grid_with_race["race_session_key"]
    grid_with_race = grid_with_race.drop(columns=[c for c in ["race_session_key"] if c in grid_with_race.columns])

    grids_clean = grid_with_race[["position", "driver_number", "session_key"]].copy()

    # Clean list-type columns in laps for SQLite
    for col in laps_clean.columns:
        if laps_clean[col].apply(lambda x: isinstance(x, list)).any():
            laps_clean[col] = laps_clean[col].astype(str)

    print("\nData Transformed Summary:")
    print("circuits:", circuits.shape)
    print("race_sessions:", race_sessions_clean.shape)
    print("teams:", teams_clean.shape)
    print("team_seasons:", team_seasons.shape)
    print("drivers_identity:", drivers_identity.shape)
    print("driver_sessions:", driver_sessions.shape)
    print("results:", results_clean.shape)
    print("pitstops:", pitstops_clean.shape)
    print("stints:", stints_clean.shape)
    print("laps:", laps_clean.shape)
    print("weather:", weather_clean.shape)
    print("race_control:", race_control_clean.shape)
    print("position:", position_clean.shape)
    print("grids:", grids_clean.shape)

    return {
        "circuits": circuits,
        "race_sessions": race_sessions_clean,
        "teams": teams_clean,
        "team_seasons": team_seasons,
        "drivers_identity": drivers_identity,
        "driver_sessions": driver_sessions,
        "results": results_clean,
        "pitstops": pitstops_clean,
        "stints": stints_clean,
        "laps": laps_clean,
        "weather": weather_clean,
        "race_control": race_control_clean,
        "position": position_clean,
        "grids": grids_clean,
    }


# -----------------------------
# LOAD (SCHEMA + INSERT)
# -----------------------------

def create_schema(conn):
    """
    Drop existing tables (if any) and recreate schema.
    """
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")

    tables = [
        "position",
        "grids",
        "race_control",
        "weather",
        "laps",
        "stints",
        "pitstops",
        "results",
        "driver_sessions",
        "drivers_identity",
        "team_seasons",
        "teams",
        "race_sessions",
        "circuits",
        "tyre_changes"
    ]

    for t in tables:
        cur.execute(f"DROP TABLE IF EXISTS {t};")

    cur.execute("""
    CREATE TABLE circuits (
        circuit_key INTEGER PRIMARY KEY,
        country_code TEXT,
        circuit_short_name TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE race_sessions (
        session_key INTEGER PRIMARY KEY,
        circuit_key INTEGER,
        date_start TEXT,
        date_end TEXT,
        year INTEGER,
        FOREIGN KEY (circuit_key) REFERENCES circuits(circuit_key)
    );
    """)

    cur.execute("""
    CREATE TABLE teams (
        team_id INTEGER PRIMARY KEY,
        team_name TEXT,
        team_colour TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE team_seasons (
        team_id INTEGER,
        team_name TEXT,
        year INTEGER,
        team_colour TEXT,
        FOREIGN KEY (team_id) REFERENCES teams(team_id)
    );
    """)

    cur.execute("""
    CREATE TABLE drivers_identity (
        driver_id INTEGER PRIMARY KEY,
        full_name TEXT,
        broadcast_name TEXT,
        name_acronym TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE driver_sessions (
        driver_number INTEGER,
        session_key INTEGER,
        team_id INTEGER,
        driver_id INTEGER,
        PRIMARY KEY (driver_number, session_key),
        FOREIGN KEY (session_key) REFERENCES race_sessions(session_key),
        FOREIGN KEY (team_id) REFERENCES teams(team_id),
        FOREIGN KEY (driver_id) REFERENCES drivers_identity(driver_id)
    );
    """)

    cur.execute("""
    CREATE TABLE results (
        session_key INTEGER,
        position INTEGER,
        driver_number INTEGER,
        number_of_laps INTEGER,
        points INTEGER,
        duration REAL,
        gap_to_leader TEXT,
        dnf BOOLEAN,
        dns BOOLEAN,
        dsq BOOLEAN,
        FOREIGN KEY (session_key) REFERENCES race_sessions(session_key)
    );
    """)

    cur.execute("""
    CREATE TABLE pitstops (
        date TEXT,
        session_key INTEGER,
        driver_number INTEGER,
        pit_duration REAL,
        lap_number INTEGER,
        FOREIGN KEY (session_key) REFERENCES race_sessions(session_key)
    );
    """)

    cur.execute("""
    CREATE TABLE stints (
        session_key INTEGER,
        stint_number INTEGER,
        driver_number INTEGER,
        lap_start INTEGER,
        lap_end INTEGER,
        compound TEXT,
        tyre_age_at_start INTEGER,
        FOREIGN KEY (session_key) REFERENCES race_sessions(session_key)
    );
    """)

    cur.execute("""
    CREATE TABLE laps (
        session_key INTEGER,
        driver_number INTEGER,
        lap_number INTEGER,
        date_start TEXT,
        duration_sector_1 REAL,
        duration_sector_2 REAL,
        duration_sector_3 REAL,
        i1_speed REAL,
        i2_speed REAL,
        is_pit_out_lap INTEGER,
        lap_duration REAL,
        segments_sector_1 TEXT,
        segments_sector_2 TEXT,
        segments_sector_3 TEXT,
        st_speed REAL,
        FOREIGN KEY (session_key) REFERENCES race_sessions(session_key)
    );
    """)

    cur.execute("""
    CREATE TABLE weather (
        date TEXT,
        session_key INTEGER,
        wind_direction INTEGER,
        wind_speed REAL,
        rainfall INTEGER,
        track_temperature REAL,
        air_temperature REAL,
        humidity REAL,
        pressure REAL,
        FOREIGN KEY (session_key) REFERENCES race_sessions(session_key)
    );
    """)

    cur.execute("""
    CREATE TABLE race_control (
        session_key INTEGER,
        date TEXT,
        driver_number INTEGER,
        lap_number INTEGER,
        category TEXT,
        flag TEXT,
        scope TEXT,
        sector INTEGER,
        message TEXT,
        FOREIGN KEY (session_key) REFERENCES race_sessions(session_key)
    );
    """)

    cur.execute("""
    CREATE TABLE grids (
        position INTEGER,
        driver_number INTEGER,
        session_key INTEGER,
        FOREIGN KEY (session_key) REFERENCES race_sessions(session_key)
    );
    """)

    cur.execute("""
    CREATE TABLE position (
        date TEXT,
        session_key INTEGER,
        driver_number INTEGER,
        position INTEGER,
        FOREIGN KEY (session_key) REFERENCES race_sessions(session_key)
    );
    """)

    conn.commit()


def load_data(conn, transformed):
    """
    Load transformed DataFrames into SQLite.
    """
    transformed["circuits"].to_sql("circuits", conn, if_exists="append", index=False)
    transformed["race_sessions"].to_sql("race_sessions", conn, if_exists="append", index=False)
    transformed["teams"].to_sql("teams", conn, if_exists="append", index=False)
    transformed["team_seasons"].to_sql("team_seasons", conn, if_exists="append", index=False)
    transformed["drivers_identity"].to_sql("drivers_identity", conn, if_exists="append", index=False)
    transformed["driver_sessions"].to_sql("driver_sessions", conn, if_exists="append", index=False)
    transformed["results"].to_sql("results", conn, if_exists="append", index=False)
    transformed["pitstops"].to_sql("pitstops", conn, if_exists="append", index=False)
    transformed["stints"].to_sql("stints", conn, if_exists="append", index=False)
    transformed["laps"].to_sql("laps", conn, if_exists="append", index=False)
    transformed["weather"].to_sql("weather", conn, if_exists="append", index=False)
    transformed["race_control"].to_sql("race_control", conn, if_exists="append", index=False)
    transformed["grids"].to_sql("grids", conn, if_exists="append", index=False)
    transformed["position"].to_sql("position", conn, if_exists="append", index=False)

    conn.commit()


# -----------------------------
# MAIN ENTRY POINT
# -----------------------------

def run_etl(db_path="/opt/airflow/data/f1_data.db", start_year=2023, sleep_sec=0.15):
    """
    Full ETL: extract from OpenF1, transform, and load into SQLite.
    """
    raw = extract_data(start_year=start_year, sleep_sec=sleep_sec)
    transformed = transform_data(raw)

    conn = sqlite3.connect(db_path)
    try:
        create_schema(conn)
        load_data(conn, transformed)
    finally:
        conn.close()

    print(f"{db_path} created successfully and all tables loaded.")


if __name__ == "__main__":
    run_etl()
    