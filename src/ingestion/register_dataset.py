from sqlalchemy import text
import sys
import os
import logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db_connect import get_engine
from logger import get_logger

logger = get_logger("register_dataset")


def register_dataset(
    source: str,
    schema_version: str = "1.0",
    update_frequency: str = "unknown",
    trust_level: str = "medium",
    notes: str = ""
) -> int:
   
    print("\n========== DATASET REGISTRATION STARTED ==========")
    print(f"Registering dataset: {source}")

    logger.info(f"Registering dataset: {source}")
   
    engine = get_engine()

    query = text("""
        INSERT INTO dataset_registry
        (source, schema_version, update_frequency, trust_level, notes)
        VALUES
        (:source, :schema_version, :update_frequency, :trust_level, :notes)
        RETURNING dataset_id
    """)

    with engine.connect() as conn:
        result = conn.execute(query,{
                "source":           source,
                "schema_version":   schema_version,
                "update_frequency": update_frequency,
                "trust_level":      trust_level,
                "notes":            notes
            }
        )
        
        conn.commit()
        dataset_id = result.fetchone()[0]
        
        print(f"Dataset registered successfully → dataset_id: {dataset_id}")
        logger.info(f"{source} registered with dataset_id={dataset_id}")
        
        print("========== DATASET REGISTRATION COMPLETED ==========")

        return dataset_id
