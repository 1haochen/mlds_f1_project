import requests
import time
import pandas as pd
import sqlite3

BASE = "https://api.openf1.org/v1"

# Safe fetch function (handles 429 rate limits)
def fetch(endpoint, params=None, max_retries=5):
    for attempt in range(max_retries):
        try:
            r = requests.get(f"{BASE}/{endpoint}", params=params)

            if r.status_code == 429:
                wait = 2 ** attempt
                print(f"!! 429 Too Many Requests → waiting {wait}s...")
                time.sleep(wait)
                continue

            r.raise_for_status()
            return pd.DataFrame(r.json())

        except Exception as e:
            wait = 2 ** attempt
            print(f"Error: {e} → retrying in {wait}s...")
            time.sleep(wait)

    raise Exception(f"Failed to fetch {endpoint} after retries.")


# 1. Load ALL meetings (year filter kept but variable renamed)
 
meetings = fetch("meetings")
###change year filter here!!!
filtered_meetings = meetings[meetings["year"] >= 2023].reset_index(drop=True)

print("Meetings:", filtered_meetings.shape)

meeting_keys = filtered_meetings["meeting_key"].unique()


# 2. Load ALL Race sessions for these meetings
sessions = fetch("sessions")
filtered_sessions = sessions[
    (sessions["meeting_key"].isin(meeting_keys)) &
    (sessions["session_name"] == "Race")
].reset_index(drop=True)

print("Sessions:", filtered_sessions.shape)

session_keys = filtered_sessions["session_key"].unique()


# -------------------------------------------------------
# 3. Load all datasets for ALL session keys
# -------------------------------------------------------
all_results, all_pit, all_stints, all_laps = [], [], [], []
all_rc, all_weather = [], []

for sk in session_keys:
    print(f"\n🔵 Loading session_key = {sk}")
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

    # Weather
    df = fetch("weather", {"session_key": sk})
    if not df.empty:
        df["session_key"] = sk
        all_weather.append(df)


# 4. Combine all tables
results = pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()
pitstops = pd.concat(all_pit, ignore_index=True) if all_pit else pd.DataFrame()
stints = pd.concat(all_stints, ignore_index=True) if all_stints else pd.DataFrame()
laps = pd.concat(all_laps, ignore_index=True) if all_laps else pd.DataFrame()
race_control = pd.concat(all_rc, ignore_index=True) if all_rc else pd.DataFrame()
weather = pd.concat(all_weather, ignore_index=True) if all_weather else pd.DataFrame()

# Drivers metadata
drivers = fetch("drivers")
drivers = drivers[drivers["session_key"].isin(session_keys)].reset_index(drop=True)


# 5. Save into a SQLite database
DB_PATH = "f1_data.db"
conn = sqlite3.connect(DB_PATH)

# Convert list-like columns to strings for SQLite compatibility
for col in laps.columns:
    if laps[col].apply(lambda x: isinstance(x, list)).any():
        laps[col] = laps[col].astype(str)

filtered_meetings.to_sql("meetings", conn, if_exists="replace", index=False)
filtered_sessions.to_sql("sessions", conn, if_exists="replace", index=False)
results.to_sql("results", conn, if_exists="replace", index=False)
pitstops.to_sql("pitstops", conn, if_exists="replace", index=False)
stints.to_sql("stints", conn, if_exists="replace", index=False)
laps.to_sql("laps", conn, if_exists="replace", index=False)
race_control.to_sql("race_control", conn, if_exists="replace", index=False)
weather.to_sql("weather", conn, if_exists="replace", index=False)
drivers.to_sql("drivers", conn, if_exists="replace", index=False)

conn.close()

print("\n DATABASE SAVED:", DB_PATH)


# Summary
print("\n========== FINAL SUMMARY ==========")
print("Meetings:", filtered_meetings.shape)
print("Sessions:", filtered_sessions.shape)
print("Results:", results.shape)
print("Pit Stops:", pitstops.shape)
print("Stints:", stints.shape)
print("Laps:", laps.shape)
print("Race Control:", race_control.shape)
print("Weather:", weather.shape)
print("Drivers:", drivers.shape)
print("===================================\n")
