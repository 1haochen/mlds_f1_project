import requests
import time
import pandas as pd
import sqlite3
from tqdm import tqdm


BASE = "https://api.openf1.org/v1"

# Safe fetch function (handles 429 rate limits)
def fetch(endpoint, params=None, max_retries=5):
    for attempt in range(max_retries):
        try:
            r = requests.get(f"{BASE}/{endpoint}", params=params)

            if r.status_code == 429:
                wait = 2 ** attempt
                #print(f"!! 429 Too Many Requests → waiting {wait}s...")
                time.sleep(wait)
                continue

            r.raise_for_status()
            return pd.DataFrame(r.json())

        except Exception as e:
            wait = 2 ** attempt
            print(f"Error: {e} → retrying in {wait}s...")
            time.sleep(wait)

    raise Exception(f"Failed to fetch {endpoint} after retries.")
# -----------------------------
# EXTRACTING PART

# 1. Extracting Datasets for ALL Meetings from 2023 onwards
 
meetings = fetch("meetings")
# ---- change year if needed ----
# the actual F1 data starts from 2023 onwards in OpenF1
filtered_meetings = meetings[meetings["year"] >= 2023].reset_index(drop=True)

print("Meetings:", filtered_meetings.shape)

meeting_keys = filtered_meetings["meeting_key"].unique()


# 2. Load ALL Race sessions for these meetings
sessions = fetch("sessions")
filtered_sessions = sessions[
    (sessions["meeting_key"].isin(meeting_keys)) &
    (sessions["session_name"] == "Race") # keeps only the race sessions
].reset_index(drop=True)

print("Sessions:", filtered_sessions.shape) # session count might be less than meetings count since we are only counting actual race, there might be meetings that are test

session_keys = filtered_sessions["session_key"].unique()

# 3. Load all datasets for ALL session keys

all_results, all_pit, all_stints, all_laps = [], [], [], []
all_rc, all_weather = [], []
all_grid = []
all_position = []


print(f"\nLoading data for {len(session_keys)} race sessions...")
for sk in tqdm(session_keys, desc="Fetching session data", unit="session"):
    time.sleep(0.15)   # reduce rate-limit risk

    # Results
    df = fetch("session_result", {"session_key": sk})
    if not df.empty:
        df["session_key"] = sk
        all_results.append(df)

    # Pit Stops
    df = fetch("pit", {"session_key": sk})
    if not df.empty:
        df["session_key"] = sk
        all_pit.append(df)

    # Stints
    df = fetch("stints", {"session_key": sk})
    if not df.empty:
        df["session_key"] = sk
        all_stints.append(df)

    # Laps
    df = fetch("laps", {"session_key": sk})
    if not df.empty:
        df["session_key"] = sk
        all_laps.append(df)

    # Race Control
    df = fetch("race_control", {"session_key": sk})
    if not df.empty:
        df["session_key"] = sk
        all_rc.append(df)
    
    # Starting grid (starting position)
    df = fetch("starting_grid", {"session_key":sk})
    if not df.empty:
        df["session_key"] = sk
        all_grid.append(df)

    # Weather
    df = fetch("weather", {"session_key": sk})
    if not df.empty:
        df["session_key"] = sk
        all_weather.append(df)

    # Position
    df = fetch("position", {"session_key": sk})
    if not df.empty:
        df["session_key"] = sk
        all_position.append(df)


# extracting grid (start) position data, stored in qualifying sessions
qual_sessions = sessions[
    (sessions["meeting_key"].isin(meeting_keys)) &
    (sessions["session_name"] == "Qualifying") # keeps only the race sessions
].reset_index(drop=True)

qual_session_keys = qual_sessions["session_key"].unique()
print(f"\nLoading grid data from {len(qual_session_keys)} qualifying sessions...")
for sk in tqdm(qual_session_keys, desc="Fetching quali grid", unit="session"):
    time.sleep(0.15)   # reduce rate-limit risk

    # Starting grid (starting position)
    df = fetch("starting_grid", {"session_key":sk})
    if not df.empty:
        df["session_key"] = sk
        all_grid.append(df)


# By default date_start is null for lap 1, here we fill it with the earliest lap2 start time.
for l in all_laps:
    l["date_start"] = pd.to_datetime(l["date_start"], format="ISO8601", utc=True)

    # Fix missing date_start for lap 1
    min_ts = l["date_start"].dropna().min()

    l.loc[
        (l["lap_number"] == 1) & (l["date_start"].isna()),
        "date_start"
    ] = min_ts

# 4. Combine all tables
results = pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()
pitstops = pd.concat(all_pit, ignore_index=True) if all_pit else pd.DataFrame()
stints = pd.concat(all_stints, ignore_index=True) if all_stints else pd.DataFrame()
laps = pd.concat(all_laps, ignore_index=True) if all_laps else pd.DataFrame()
race_control = pd.concat(all_rc, ignore_index=True) if all_rc else pd.DataFrame()
weather = pd.concat(all_weather, ignore_index=True) if all_weather else pd.DataFrame()
grid = pd.concat(all_grid, ignore_index=True) if all_grid else pd.DataFrame()
position = pd.concat(all_position, ignore_index=True) if all_position else pd.DataFrame()

# Drivers metadata
drivers = fetch("drivers")
drivers = drivers[drivers["session_key"].isin(session_keys)].reset_index(drop=True)

# Print summary for extracted data
print("\nData Extraction Summary:")
print("Results:", results.shape)
print("Pit Stops:", pitstops.shape)
print("Stints:", stints.shape)
print("Laps:", laps.shape)
print("Race Control:", race_control.shape)
print("Weather:", weather.shape)
print("Grid:", grid.shape)

# -----------------------------
# TRANSFORMING PART
# [1. Clean circuits data]
circuits = filtered_sessions[["circuit_key", "country_code", "circuit_short_name"]].drop_duplicates().reset_index(drop=True)

# [2. Clean race sessions data]
race_sessions_clean = filtered_sessions[["session_key", "circuit_key", "date_start", "date_end", "year"]]

# [3. Clean team related data]
# ----- TEAM_LINEAGE Needs to be manually updated each year as team name might change, or new team added -----
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
# Get teams data from drivers and sessions
teams = drivers.merge(
    filtered_sessions[["session_key", "year"]],
    on="session_key",
    how="left"
)[["team_name", "team_colour", "year"]].copy()

# Normalize team_colour to lowercase
teams["team_colour"] = teams["team_colour"].str.lower()

# Drop duplicates and reset index
teams = teams.drop_duplicates().reset_index(drop=True)
# Step 1 — Convert lineage dictionary to DataFrame)
lineage_df = (
    pd.DataFrame.from_dict(TEAM_LINEAGE, orient="index", columns=["team_id"])
    .reset_index()
    .rename(columns={"index": "team_name"})
)
# Step 2 — Attach team_id to each (team, year)
teams_with_id = teams.merge(lineage_df, on="team_name", how="left")

# Step 3 — teams table (stable identifier & latest name and colour)
teams_clean = (
    teams_with_id[["team_id", "team_name"]]
    .drop_duplicates(subset=["team_id"])
    .sort_values("team_id")
    .reset_index(drop=True)
)
teams_clean = (
    teams_with_id
    .sort_values(["team_id", "year"])                 # newest year last
    .groupby("team_id")                               # group teams
    .tail(1)                                           # take the newest name
    [["team_id", "team_name","team_colour"]]                        # keep only these columns
    .sort_values("team_id")                           # final order
    .reset_index(drop=True)
)

# Step 4 — team_seasons table (historic team name & colour by year)
team_seasons = (
    teams_with_id[["team_id", "team_name", "year", "team_colour"]]
    .drop_duplicates()
    .sort_values(["team_id", "year"])
    .reset_index(drop=True)
)

# [4. Clean drivers data]

# merge team id on drivers
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
    .sort_values(["driver_number", "team_id","year"])
    .reset_index(drop=True)
) 

## Create the stable drivers table, table for unique driver identity
# Normalize name fields
drivers_clean["full_name"] = drivers_clean["full_name"].str.strip()
drivers_clean["broadcast_name"] = drivers_clean["broadcast_name"].str.strip()
drivers_clean["name_acronym"] = drivers_clean["name_acronym"].str.strip()

# Stable unique driver identity
drivers_identity = (
    drivers_clean[
        ["full_name", "broadcast_name", "name_acronym"]
    ]
    .drop_duplicates()
    .reset_index(drop=True)
)

# Assign driver_id
drivers_identity["driver_id"] = drivers_identity.index + 1 # driver number not included in this table, as it might changes over years
# driver session table (drivers might change team within each season)
driver_sessions = drivers_merge.merge(
    drivers_identity,
    on=["full_name", "broadcast_name", "name_acronym"],
    how="left"
)[["driver_number", "session_key", "team_id", "driver_id"]].drop_duplicates().reset_index(drop=True)

# [5. Clean weather data]
# weather data cleaning, remove meeting_key
weather_clean = weather.drop(columns=["meeting_key"])

# [6. Clean race_control data]
# race_control data cleaning, remove meeting_key
race_control_clean = race_control.drop(columns=["meeting_key"])


# [7. Clean results data] - for ending positions
# convert position, number of laps, points to integer
results["position"] = pd.to_numeric(results["position"], errors="coerce").astype("Int64")
results["number_of_laps"] = pd.to_numeric(results["number_of_laps"], errors="coerce").astype("Int64")
results["points"] = pd.to_numeric(results["points"], errors="coerce").astype("Int64")
# results status, categorize as cleanup, finish, dnf, dns, dsq
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
]]

# [8. Clean pitstops data]
pitstops_clean = pitstops.drop(columns=["meeting_key"])

# [9. Clean stints data]
stints_clean = stints.drop(columns=["meeting_key"])

# [10. Clean laps data]
laps_clean = laps.drop(columns=["meeting_key"])

# Clean position data
position_clean = position.drop(columns=["meeting_key"])

# [11. Clean grids data] - for starting positions

# If a driver appears in the grid but not in the race results, they did not participate
# If a driver appears in the race results but not in the grid, 
# they started from the pit lane and have the last grid position.

# Extract race session_key per meeting_key
race_map = (
    sessions[
        sessions["session_name"].str.lower() == "race"
    ][["meeting_key", "session_key"]]
    .rename(columns={"session_key": "race_session_key"})
    .drop_duplicates("meeting_key")
)

# Merge to grid
grid_with_race = grid.merge(race_map, on="meeting_key", how="left")

# Replace qual session_key with race session_key
grid_with_race["session_key"] = grid_with_race["race_session_key"]

# Drop helper column
grid_with_race = grid_with_race.drop(columns=["race_session_key"])
grid_with_race

grids_clean = grid_with_race[["position", "driver_number", "session_key"]]

# Print summary for transformed data
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

# -----------------------------
# LOADING PART

# Create SQLite database
conn = sqlite3.connect("f1_data.db")
cur = conn.cursor()

# Enable foreign keys
cur.execute("PRAGMA foreign_keys = ON;")

# DROP existing tables (optional clean start)
tables = [
    "circuits", "race_sessions", "teams", "team_seasons",
    "drivers_identity", "driver_sessions",
    "results", "pitstops", "stints", "laps",
    "weather", "race_control", "grids", "position"
]

for t in tables:
    cur.execute(f"DROP TABLE IF EXISTS {t};")

# CREATE TABLE statements

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

# CLEAN: Convert list-type columns into strings for SQLite

for col in laps_clean.columns:
    if laps_clean[col].apply(lambda x: isinstance(x, list)).any():
        laps_clean[col] = laps_clean[col].astype(str)


# LOAD TABLES into SQLite
circuits.to_sql("circuits", conn, if_exists="append", index=False)
race_sessions_clean.to_sql("race_sessions", conn, if_exists="append", index=False)
teams_clean.to_sql("teams", conn, if_exists="append", index=False)
team_seasons.to_sql("team_seasons", conn, if_exists="append", index=False)
drivers_identity.to_sql("drivers_identity", conn, if_exists="append", index=False)
driver_sessions.to_sql("driver_sessions", conn, if_exists="append", index=False)
results_clean.to_sql("results", conn, if_exists="append", index=False)
pitstops_clean.to_sql("pitstops", conn, if_exists="append", index=False)
stints_clean.to_sql("stints", conn, if_exists="append", index=False)
laps_clean.to_sql("laps", conn, if_exists="append", index=False)
weather_clean.to_sql("weather", conn, if_exists="append", index=False)
race_control_clean.to_sql("race_control", conn, if_exists="append", index=False)
grids_clean.to_sql("grids", conn, if_exists="append", index=False)
position_clean.to_sql("position", conn, if_exists="append", index=False)

