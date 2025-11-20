# update_f1_data.py
import sqlite3
import subprocess
import sys
import pandas as pd
import time
from tqdm import tqdm

# Reuse your ETL functions
from load_f1_functional import (
    fetch,
    get_race_sessions,
    get_qual_sessions,
    transform_data,
)

DB_PATH = "f1_data.db"


# 1. Read existing sessions in DB
def get_existing_session_keys(conn):
    try:
        df = pd.read_sql("SELECT session_key FROM race_sessions;", conn)
        return df["session_key"].tolist()
    except Exception:
        return []


# 2. Detect new race + qualifying sessions from OpenF1
def get_new_sessions(start_year, existing_session_keys):
    """
    Returns:
        new_race_sessions
        new_qual_sessions
        all_sessions (full session table)
        filtered_meetings
    """
    filtered_meetings, all_sessions, race_sessions = get_race_sessions(start_year)

    # Identify race sessions not yet loaded in DB
    new_race = race_sessions[
        ~race_sessions["session_key"].isin(existing_session_keys)
    ].reset_index(drop=True)

    if new_race.empty:
        return new_race, pd.DataFrame(), all_sessions, filtered_meetings

    # Identify matching qualifying sessions for the same meetings
    meeting_keys = new_race["meeting_key"].unique()
    new_qual = all_sessions[
        (all_sessions["meeting_key"].isin(meeting_keys)) &
        (all_sessions["session_name"] == "Qualifying")
    ].reset_index(drop=True)

    return new_race, new_qual, all_sessions, filtered_meetings


# 3. Extract all data for NEW sessions only
def extract_new_session_data(new_race, new_qual, all_sessions, sleep_sec=0.15):
    """
    Extracts all race and qualifying grid data for new sessions only.
    Returns raw dict ready to pass into transform_data().
    """
    race_keys = new_race["session_key"].tolist()
    qual_keys = new_qual["session_key"].tolist()

    collect_results, collect_pit, collect_stints, collect_laps = [], [], [], []
    collect_rc, collect_weather, collect_position, collect_grid = [], [], [], []

    print(f"\nExtracting NEW race sessions: {len(race_keys)}")
    for sk in tqdm(race_keys, desc="Race sessions"):
        tqdm.write(f"Fetching session_key = {sk}") 
        time.sleep(sleep_sec)
        
        for endpoint, bucket in [
            ("session_result", collect_results),
            ("pit", collect_pit),
            ("stints", collect_stints),
            ("laps", collect_laps),
            ("race_control", collect_rc),
            ("weather", collect_weather),
            ("position", collect_position),
            ("starting_grid", collect_grid),
        ]:
            df = fetch(endpoint, {"session_key": sk})
            if not df.empty:
                df["session_key"] = sk
                bucket.append(df)

    print(f"\nExtracting NEW qualifying sessions: {len(qual_keys)}")
    for sk in tqdm(qual_keys, desc="Qual sessions"):
        tqdm.write(f"Fetching session_key = {sk}") 
        time.sleep(sleep_sec)

        df = fetch("starting_grid", {"session_key": sk})
        if not df.empty:
            df["session_key"] = sk
            collect_grid.append(df)

    # Combine helpers
    def combine(lst):
        return pd.concat(lst, ignore_index=True) if lst else pd.DataFrame()

    raw = {
        "race_sessions": new_race,
        "sessions": all_sessions,   # needed for grid mapping
        "qual_sessions": new_qual,
        "results": combine(collect_results),
        "pitstops": combine(collect_pit),
        "stints": combine(collect_stints),
        "laps": combine(collect_laps),
        "race_control": combine(collect_rc),
        "weather": combine(collect_weather),
        "position": combine(collect_position),
        "grid": combine(collect_grid),
    }

    # Drivers for these new sessions only
    all_drivers = fetch("drivers")
    raw["drivers"] = all_drivers[
        all_drivers["session_key"].isin(race_keys)
    ].reset_index(drop=True)

    return raw



# 4. Insert transformed data (append-only)
def safe_append(conn, df, table, pk_cols):
    """
    Append df into table, but skip rows whose primary key already exists.
    
    pk_cols: list of primary key columns (one or more)
    """
    if df.empty:
        return
    
    # 1. Read existing keys from DB
    pk_query = f"SELECT {', '.join(pk_cols)} FROM {table};"
    try:
        existing = pd.read_sql(pk_query, conn)
    except Exception:
        existing = pd.DataFrame(columns=pk_cols)

    # 2. Remove duplicates
    merged = df.merge(existing, on=pk_cols, how="left", indicator=True)
    new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

    if not new_rows.empty:
        new_rows.to_sql(table, conn, if_exists="append", index=False)


def append_to_db(conn, transformed):
    
    # table : primary key(s)
    pk_map = {
        "circuits": ["circuit_key"],
        "race_sessions": ["session_key"],
        "teams": ["team_id"],
        "team_seasons": ["team_id", "year"],
        "drivers_identity": ["driver_id"],
        "driver_sessions": ["driver_number", "session_key"],
        "results": ["session_key", "driver_number"],
        "pitstops": [],  # no PK → append directly
        "stints": [],    # no PK → append directly
        "laps": [],      # no PK → append directly
        "weather": [],   # no PK → append directly
        "race_control": [],   # no PK → append directly
        "position": ["session_key", "driver_number", "date"],
        "grids": ["session_key", "driver_number"],
    }

    for table, pk_cols in pk_map.items():
        df = transformed.get(table)
        if df is None or df.empty:
            continue

        if pk_cols:
            # Use safe append
            safe_append(conn, df, table, pk_cols)
        else:
            # Tables without PK constraints → append normally
            df.to_sql(table, conn, if_exists="append", index=False)

    conn.commit()



# 5. MAIN INCREMENTAL UPDATE ENTRY POINT
def update_f1_data(db_path=DB_PATH, start_year=2023, sleep_sec=0.15):
    conn = sqlite3.connect(db_path)

    try:
        print("\n Reading existing session keys from DB...")
        existing_session_keys = get_existing_session_keys(conn)

        print("\n Getting new sessions from OpenF1...")
        new_race, new_qual, all_sessions, filtered_meetings = get_new_sessions(
            start_year,
            existing_session_keys
        )

        if new_race.empty:
            print("\n Database is already up to date — no new race sessions.")
            return

        print(f"\n Found {len(new_race)} new race sessions.")

        print("\n Extracting NEW session data...")
        raw_new = extract_new_session_data(new_race, new_qual, all_sessions, sleep_sec)

        print("\n Transforming NEW session data...")
        transformed = transform_data(raw_new)

        print("\n Inserting NEW data into database...")
        append_to_db(conn, transformed)

        print("\n Update completed successfully — DB is now up to date!")
        # NEW: Run create_tyre_changes.py automatically
        print("\n Running create_tyre_changes.py to update tyre-change table...")

        try:
            subprocess.run(
                [sys.executable, "create_tyre_changes.py"], 
                check=True
            )
            print("\n Tyre changes updated successfully!")
        except subprocess.CalledProcessError as e:
            print("\n ⚠ ERROR running create_tyre_changes.py:")
            print(e)

    finally:
        conn.close()



if __name__ == "__main__":
    update_f1_data()
