import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from sqlalchemy import text
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
            return pd.Timestamp(year=year, month=6,  day=1).isoformat()
        elif "post" in season:
            return pd.Timestamp(year=year, month=11, day=1).isoformat()
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
    id_cols = ["Well_ID", "Latitude", "Longitude", "Site_Name",
               "State_Name_With_LGD_Code", "Aquifer"]

    # Melt: wide → long
    df_long = df[id_cols + season_cols].melt(
        id_vars    = id_cols,
        value_vars = season_cols,
        var_name   = "season_label",
        value_name = "raw_value"
    )
    logger.info(f"  Rows after melt (well  season): {len(df_long)}")

    signals = []
    skipped_no_coords  = 0
    skipped_no_timestamp = 0

    for _, row in df_long.iterrows():
        # --- Coordinates ---
        lat = row.get("Latitude")
        lon = row.get("Longitude")
        if pd.isna(lat) or pd.isna(lon):
            skipped_no_coords += 1
            continue

        # --- Timestamp from season column name ---
        timestamp = _season_to_timestamp(row["season_label"])
        if timestamp is None:
            skipped_no_timestamp += 1
            continue

        # --- Normalized value ---
        # "Dry" = well was dry that season → NULL (not 0, not filled)
        # NaN   = not measured             → NULL
        raw_val = row.get("raw_value")
        if pd.isna(raw_val) or str(raw_val).strip().lower() == "dry":
            normalized_value = None
        else:
            try:
                normalized_value = float(raw_val)
            except (ValueError, TypeError):
                normalized_value = None

        # Compact season label for source_id e.g. "pre_2015"
        short_season = row["season_label"].split("(")[0].strip()\
                                          .replace("-monsoon_", "_")\
                                          .replace("Pre_", "pre_")\
                                          .replace("Post_", "post_")\
                                          .lower()

        source_id = f"{row['Well_ID']}_{short_season}"

        signals.append({
            "source_id":        source_id,
            "timestamp":        timestamp,
            "latitude":         float(lat),
            "longitude":        float(lon),
            "feature_type":     "groundwater_level",
            "normalized_value": normalized_value,
            "source_reference": SOURCE_REF,
            "dataset_id":       dataset_id
        })

    logger.info(f"  Signals prepared : {len(signals)}")
    logger.info(f"  Skipped (no coords)    : {skipped_no_coords}")
    logger.info(f"  Skipped (no timestamp) : {skipped_no_timestamp}")

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

    logger.info(f" Inserted {inserted} signals from groundwater dataset.")
    return inserted
