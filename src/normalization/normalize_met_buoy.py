import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


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

    # Drop empty columns
    df = df.dropna(axis=1, how="all")

    # Ensure timestamp
    df = df.dropna(subset=["timestamp"])

    def parse_ts(val):
        try:
            return pd.Timestamp(val)
        except:
            return None

    df["timestamp"] = df["timestamp"].apply(parse_ts)
    df = df.dropna(subset=["timestamp"])

    # Normalize value
    def clean_value(val):
        if pd.isna(val):
            return None
        try:
            return float(val)
        except:
            return None
        
    df["normalized_value"] = df["water_temp"].apply(clean_value)

    # Columns
    df["source_id"] = "NOAA_51001_" + df["timestamp"].astype(str)
    df["latitude"] = BUOY_51001_LAT
    df["longitude"] = BUOY_51001_LON

    df["feature_type"] = "meteorological_buoy_reading"
    df["source_reference"] = BUOY_51001_REF
    df["dataset_id"] = dataset_id

    # Confidence
    df["confidence_score"] = 0.85

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
        logger.warning("No valid meteorological buoy signals.")
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

    logger.info(f"Inserted {len(df_final)} meteorological buoy signals")

    return len(df_final)
    
