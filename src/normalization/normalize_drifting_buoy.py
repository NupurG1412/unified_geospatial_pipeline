import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from sqlalchemy import text
from src.db_connect import get_engine
from src.logger import get_logger

logger = get_logger("normalize_drifting_buoy")

def normalize_drifting_buoy(filepath, dataset_id):

    logger.info(f"Loading drifting buoy file: {filepath}")

    df = pd.read_csv(filepath)

    logger.info(f"  Raw rows loaded: {len(df)}")
    print(f"Rows loaded: {len(df)}")

    signals = []
    skipped_no_timestamp = 0
    skipped_no_coords = 0

    for _, row in df.iterrows():
        # --- Timestamp parsing ---
        try:

            time_int = int(row["time"])
            hour = time_int // 100
            minute = time_int % 100

            timestamp = pd.Timestamp(
                year=int(row["year"]),
                month=int(row["month"]),
                day=int(row["day"]),
                hour=hour,
                minute=minute
            ).isoformat()

        except Exception:
            skipped_no_timestamp += 1
            continue

        # --- Coordinates ---
        lat = row.get("latitude")
        lon = row.get("longitude")

        if pd.isna(lat) or pd.isna(lon):
            skipped_no_coords += 1
            continue

        # --- Normalized value ---
        raw_val = row.get("water_temp")

        if raw_val == "MM" or pd.isna(raw_val):
            normalized_value = None
        else:
            try:
                normalized_value = float(raw_val)
            except (ValueError, TypeError):
                normalized_value = None

        # --- Source ID ---
        source_id = f"DRIFTING_{row['year']}{row['month']}{row['day']}_{row['time']}"

        # --- Append signal ---
        signals.append({
            "source_id":        source_id,
            "timestamp":        timestamp,
            "latitude":         float(lat),
            "longitude":        float(lon),
            "feature_type":     "drifting_buoy_reading",
            "normalized_value": normalized_value,
            "source_reference": "NOAA Drifting Buoy",
            "dataset_id":       dataset_id
        })


    # --- Logging summary ---
    logger.info(f"  Signals prepared : {len(signals)}")
    logger.info(f"  Skipped (no timestamp) : {skipped_no_timestamp}")
    logger.info(f"  Skipped (no coords)    : {skipped_no_coords}")

    if not signals:
        logger.warning("  No valid signals to insert.")
        return 0
    
    # --- Insert into database ---
    engine = get_engine()
    inserted = 0

    with engine.connect() as conn:
        for sig in signals:
            conn.execute(
                text("""
                    INSERT INTO marine_signals
                        (source_id, timestamp, latitude, longitude, geom,
                         feature_type, normalized_value, source_reference, dataset_id)
                    VALUES (
                        :source_id,
                        :timestamp,
                        :latitude,
                        :longitude,
                        ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326)::geography,
                        :feature_type,
                        :normalized_value,
                        :source_reference,
                        :dataset_id
                    )
                """),
                sig
            )
            inserted += 1

        conn.commit()

    logger.info(f"  Inserted {inserted} signals from drifting buoy dataset.")

    return inserted