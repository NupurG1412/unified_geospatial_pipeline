import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


from src.db_connect import get_engine
from src.logger import get_logger

logger = get_logger("normalize_drifting_buoy")

def normalize_drifting_buoy(filepath, dataset_id):

    logger.info(f"Loading drifting buoy file: {filepath}")
    
    df = pd.read_csv(filepath)

    logger.info(f"  Raw rows loaded: {len(df)}")
    print(f"Rows loaded: {len(df)}")

    # Timestamp
    def parse_timestamp(row):
        try:
            time_int = int(row["time"])
            hour = time_int // 100
            minute = time_int % 100

            return pd.Timestamp(
                year=int(row["year"]),
                month=int(row["month"]),
                day=int(row["day"]),
                hour=hour,
                minute=minute
            )
        except:
            return None

    df["timestamp"] = df.apply(parse_timestamp, axis=1)

    # Drop invalid rows
    df = df.dropna(subset=["timestamp", "latitude", "longitude"])

    # Normalize value
    def clean_value(val):
        if val == "MM" or pd.isna(val):
            return None
        try:
            return float(val)
        except:
            return None

    df["normalized_value"] = df["water_temp"].apply(clean_value)
    
    # Columns
    df["source_id"] = (
        "DRIFTING_" +
        df["year"].astype(str) +
        df["month"].astype(str) +
        df["day"].astype(str) +
        "_" +
        df["time"].astype(str)
    )

    df["feature_type"] = "drifting_buoy_reading"
    df["source_reference"] = "NOAA Drifting Buoy"
    df["dataset_id"] = dataset_id

     # Confidence
    df["confidence_score"] = 0.8

    # Geometry
    df["geom"] = df.apply(
        lambda row: f"POINT({row['longitude']} {row['latitude']})",
        axis=1
    )

    # Final columns
    final_cols = [
        "source_id", "timestamp", "latitude", "longitude",
        "geom", "feature_type", "normalized_value",
        "source_reference", "dataset_id", "confidence_score"
    ]

    df_final = df[final_cols]

    if df_final.empty:
        logger.warning("No valid drifting buoy signals.")
        return 0

    engine = get_engine()

    df_final = df_final.drop_duplicates(
    subset=["source_id", "timestamp", "latitude", "longitude", "feature_type"]
    )
    
    # BATCH INSERT
    df_final.to_sql(
        "marine_signals",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000
    )

    logger.info(f"Inserted {len(df_final)} drifting buoy signals")

    return len(df_final)
    