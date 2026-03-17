import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from sqlalchemy import text
from src.db_connect import get_engine
from src.logger import get_logger

logger = get_logger("normalize_met_buoy")

BUOY_51001_LAT =  23.445
BUOY_51001_LON = -162.279
BUOY_51001_REF = "NOAA NDBC Station 51001 - NW Hawaii (23.445N, 162.279W)"


def normalize_met_buoy(filepath: str, dataset_id: int) -> int:
   
    logger.info(f"Loading meteorological buoy file: {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"  Raw rows loaded: {len(df)}")

    # Drop fully-empty columns 
    empty_cols = [col for col in df.columns if df[col].isna().all()]
    if empty_cols:
        logger.info(f"  Dropping fully-empty columns: {empty_cols}")
        df.drop(columns=empty_cols, inplace=True)

    signals = []

    for _, row in df.iterrows():
        # --- Timestamp ---
        raw_ts = row.get("timestamp")
        if pd.isna(raw_ts) or str(raw_ts).strip() == "":
            logger.warning("  Skipping row with missing timestamp.")
            continue
        try:
            timestamp = pd.Timestamp(raw_ts).isoformat()
        except Exception:
            logger.warning(f"  Skipping unparseable timestamp: {raw_ts}")
            continue

        # --- Normalized value: water_temp ---
        # NaN kept as NULL 
        raw_val = row.get("water_temp")
        if pd.isna(raw_val):
            normalized_value = None
        else:
            try:
                normalized_value = float(raw_val)
            except (ValueError, TypeError):
                normalized_value = None

        source_id = f"NOAA_51001_{timestamp}"

        signals.append({
            "source_id":        source_id,
            "timestamp":        timestamp,
            "latitude":         BUOY_51001_LAT,
            "longitude":        BUOY_51001_LON,
            "feature_type":     "meteorological_buoy_reading",
            "normalized_value": normalized_value,
            "source_reference": BUOY_51001_REF,
            "dataset_id":       dataset_id
        })

    logger.info(f"  Signals prepared: {len(signals)}")

    if not signals:
        logger.warning("  No valid signals to insert.")
        return 0

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

    print(f"Inserted {inserted} meteorological buoy signals")

    logger.info(f"  Inserted {inserted} signals from meteorological buoy 51001.")
    return inserted
