import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


from src.db_connect import get_engine
from src.logger import get_logger

logger = get_logger("normalize_groundwater")

SOURCE_REF = "ATAL JAL Disclosed Ground Water Level India 2015-2022"


def _season_to_timestamp(col_name: str) -> str | None:
    
    try:
        # Extract 'Pre-monsoon_2015' part
        short = col_name.split("(")[0].strip()          # e.g. "Pre-monsoon_2015"
        parts = short.split("_")
        year  = int(parts[-1])                           # 2015
        season = parts[0].lower()                        # "pre-monsoon" or "post-monsoon"

        if "pre" in season:
            return pd.Timestamp(year=year, month=6,  day=1)
        elif "post" in season:
            return pd.Timestamp(year=year, month=11, day=1)
        else:
            return None
    except Exception:
        return None


def normalize_groundwater(filepath: str, dataset_id: int) -> int:
    
    logger.info(f"Loading groundwater file: {filepath}")
    df = pd.read_csv(filepath, encoding="latin1")
    logger.info(f"  Raw rows (wells) loaded: {len(df)}")

    # Identify the 16 seasonal columns
    season_cols = [c for c in df.columns if "monsoon" in c.lower()]
    logger.info(f"  Seasonal columns found: {len(season_cols)}")

    # ID columns to keep per well
    id_cols = ["Well_ID", "Latitude", "Longitude"]

    # Melt: wide → long
    df_long = df[id_cols + season_cols].melt(
        id_vars    = id_cols,
        value_vars = season_cols,
        var_name   = "season_label",
        value_name = "raw_value"
    )
    logger.info(f"  Rows after melt (well  season): {len(df_long)}")

    # Drop rows without coordinates
    df_long = df_long.dropna(subset=["Latitude", "Longitude"])
    
    # Timestamp
    df_long["timestamp"] = df_long["season_label"].apply(_season_to_timestamp)

    # Drop invalid timestamps
    df_long = df_long.dropna(subset=["timestamp"])

    # Normalize values
    def clean_value(val):
        if pd.isna(val) or str(val).strip().lower() == "dry":
            return None
        try:
            return float(val)
        except:
            return None
        
    df_long["normalized_value"] = df_long["raw_value"].apply(clean_value)

    # Source ID
    df_long["source_id"] = (
        df_long["Well_ID"].astype(str) + "_" +
        df_long["season_label"].str.replace(r"[^\w]", "", regex=True)
    )

    # Feature type
    df_long["feature_type"] = "groundwater_level"

    # Source reference
    df_long["source_reference"] = SOURCE_REF

    # Dataset ID
    df_long["dataset_id"] = dataset_id

    # Rename columns
    df_long.rename(columns={
        "Latitude": "latitude",
        "Longitude": "longitude"
    }, inplace=True)

    # Confidence score (rule-based)
    df_long["confidence_score"] = 0.9  # government dataset

    # Geometry column
    df_long["geom"] = df_long.apply(
        lambda row: f"POINT({row['longitude']} {row['latitude']})",
        axis=1
    )

    # Selecting final columns
    final_cols = [
        "source_id", "timestamp", "latitude", "longitude",
        "geom", "feature_type", "normalized_value",
        "source_reference", "dataset_id", "confidence_score"
    ]

    df_final = df_long[final_cols]

    logger.info(f"Final signals ready: {len(df_final)}")

    if df_final.empty:
        logger.warning("No valid signals to insert.")
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

    logger.info(f"Inserted {len(df_final)} groundwater signals")

    return len(df_final)

