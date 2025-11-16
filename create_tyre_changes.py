import sqlite3
import pandas as pd
conn = sqlite3.connect("f1_data.db")

race_sessions = pd.read_sql("""
        SELECT *
        FROM race_sessions
    """, conn)
keys = race_sessions.session_key.unique()
record_summaries = []

for session_key in keys:
    session_key = int(session_key)
    stints = pd.read_sql("""
        SELECT *
        FROM stints
        WHERE session_key = ?
        ORDER BY driver_number, stint_number
    """, conn, params=(session_key,))

    laps = pd.read_sql("""
        SELECT *
        FROM laps
        WHERE session_key = ?
    """, conn, params=(session_key,))

    position = pd.read_sql("""
        SELECT *
        FROM position
        WHERE session_key = ?
    """, conn, params=(session_key,))

    if stints.empty or laps.empty:
        print("Not enough data to compute tyre changes.")

    laps["lap_number"] = laps["lap_number"].astype(int)
    laps["lap_duration"] = laps["lap_duration"].astype(float)

    laps["date_start"] = pd.to_datetime(laps["date_start"], utc=True, errors="coerce")

    position["date"] = pd.to_datetime(position["date"], utc=True, errors="coerce")

    positions_with_lap = pd.merge_asof(
        laps.sort_values("date_start"),
        position.sort_values("date"),
        by="driver_number",
        right_on="date",
        left_on="date_start",
        direction="backward"
    )

    # 3. TYRE CHANGE ANALYSIS
    valid_types = {
        "SOFT->MEDIUM", "MEDIUM->HARD", "HARD->SOFT",
        "SOFT->HARD", "MEDIUM->SOFT", "HARD->MEDIUM",
        "SOFT->SOFT", "MEDIUM->MEDIUM", "HARD->HARD"
    }
    change_records = []
    for driver, stint_df in stints.groupby("driver_number"):
        stint_df = stint_df.sort_values("stint_number")
        for i in range(len(stint_df) - 1):
            old = stint_df.iloc[i]
            new = stint_df.iloc[i + 1]

            old_comp = old["compound"]
            new_comp = new["compound"]
            change_type = f"{old_comp}->{new_comp}"
            if change_type not in valid_types:
                continue

            tyre_change_lap = int(old["lap_end"])
            laps_on_old_tyre = int(old["lap_end"] - old["lap_start"] + 1)
            laps_on_new_tyre = min(5, int(new["lap_end"] - new["lap_start"] + 1))

            # Get available lap range for this driver
            driver_laps = positions_with_lap[positions_with_lap.driver_number == driver]

            if driver_laps.empty:
                continue

            min_lap = driver_laps.lap_number.min()
            max_lap = driver_laps.lap_number.max()

            # ---- Safe window clamping ----
            before_start = max(tyre_change_lap - 1, min_lap)
            before_end   = min(tyre_change_lap, max_lap)

            after_start  = max(tyre_change_lap + 2, min_lap)
            after_end    = min(tyre_change_lap + 5, max_lap)

            # ---- TRUE POSITION BEFORE TYRE CHANGE ----
            pos_before = driver_laps[
                (driver_laps.lap_number >= before_start) &
                (driver_laps.lap_number <= before_end)
            ]["position"].mean()

            # ---- TRUE POSITION AFTER TYRE CHANGE ----
            pos_after = driver_laps[
                (driver_laps.lap_number >= after_start) &
                (driver_laps.lap_number <= after_end)
            ]["position"].mean()
            # LAP TIME CHANGE (before vs after)
            before_laps = laps[
                (laps.driver_number == driver) &
                (laps.lap_number >= tyre_change_lap - 1) &
                (laps.lap_number <= tyre_change_lap)
            ]["lap_duration"].mean()

            after_laps = laps[
                (laps.driver_number == driver) &
                (laps.lap_number >= tyre_change_lap + 2) &
                (laps.lap_number <= tyre_change_lap + 5)
            ]["lap_duration"].mean()

            lap_time_change = None
            if pd.notnull(before_laps) and pd.notnull(after_laps):
                lap_time_change = after_laps - before_laps

            # -------------------------------------------
            # SAVE RECORD
            # -------------------------------------------
            change_records.append({
                "driver": driver,
                "change_type": change_type,
                "tyre_change_lap": tyre_change_lap,
                "laps_on_old_tyre": laps_on_old_tyre,
                "laps_on_new_tyre": laps_on_new_tyre,
                "pos_before": pos_before,
                "pos_after": pos_after,
                "position_change": None if (pos_before is None or pos_after is None)
                                        else pos_after - pos_before,
                "lap_time_change": lap_time_change
            })
    summary = pd.DataFrame(change_records)
    summary["session_key"] = session_key
    record_summaries.append(summary)


tyre_changes = pd.concat(record_summaries, ignore_index=True) if record_summaries else pd.DataFrame()
if tyre_changes is not None:
    conn.execute("DROP TABLE IF EXISTS tyre_changes;")
    tyre_changes.to_sql("tyre_changes", conn, index=False)

    conn.commit()
    print("Created full tyre_changes table.")

conn.close()
print("f1_data.db created successfully updated.")