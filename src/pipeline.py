import sys
import os
import logging

# ensure src folder available
sys.path.insert(0, os.path.dirname(__file__))

from ingestion.register_dataset import register_dataset
from normalization.normalize_drifting_buoy import normalize_drifting_buoy
from normalization.normalize_met_buoy      import normalize_met_buoy
from normalization.normalize_groundwater   import normalize_groundwater


from reporting.generate_report import generate_pipeline_report

from logger import get_logger

logger = get_logger("pipeline")


DATA_FOLDER = "data_raw"

DRIFTING_BUOY_FILE = os.path.join(DATA_FOLDER, "drifting_buoy_data.csv")
MET_BUOY_FILE      = os.path.join(DATA_FOLDER, "meteorological_buoy_data.csv")
GROUNDWATER_FILE   = os.path.join(DATA_FOLDER, "Atal_Jal_Disclosed_Ground_Water_Level-2015-2022.csv")



def run_pipeline():
    print("========== UNIFIED GEOSPATIAL SIGNAL PIPELINE STARTED ==========")
    logger.info("Pipeline started")

    results = {}

    # STEP 1 : DATASET REGISTRATION

    print("\nSTEP 1: Registering datasets in dataset_registry")
    logger.info("Registering datasets")
    
    drifting_id = register_dataset(
        source           = "NOAA Drifting Buoy - Korean Waters",
        schema_version   = "1.0",
        update_frequency = "hourly",
        trust_level      = "high",
        notes            = "Single-location drifting buoy near 37.24N, 126.02E. "
                           "MM values treated as NULL."
    )

    met_buoy_id = register_dataset(
        source           = "NOAA NDBC Station 51001 - NW Hawaii",
        schema_version   = "1.0",
        update_frequency = "10-minutely",
        trust_level      = "high",
        notes            = "Fixed station at 23.445N, 162.279W. "
                           "Lat/lon not in file; hardcoded from NOAA station metadata."
    )

    groundwater_id = register_dataset(
        source           = "ATAL JAL Disclosed Ground Water Level India 2015-2022",
        schema_version   = "1.0",
        update_frequency = "seasonal (pre/post monsoon)",
        trust_level      = "medium",
        notes            = "Wide-format dataset melted to long. "
                           "'Dry' entries kept as NULL. Covers 7 Indian states."
    )



    print("Datasets registered successfully")

    # STEP 2 : DATA NORMALIZATION
    
    print("\nSTEP 2: Normalizing datasets into unified signal schema")
    logger.info("Starting normalization stage")

    logger.info("\n--- Drifting Buoy ---")
    print("\nProcessing Drifting Buoy Dataset...")
    results["drifting_buoy"] = normalize_drifting_buoy(DRIFTING_BUOY_FILE, drifting_id)

    logger.info("\n--- Meteorological Buoy 51001 ---")
    print("\nProcessing Meteorological Buoy Dataset...")
    results["met_buoy_51001"] = normalize_met_buoy(MET_BUOY_FILE, met_buoy_id)

    logger.info("\n--- ATAL JAL Groundwater ---")
    print("\nProcessing Groundwater Dataset...")
    results["groundwater"] = normalize_groundwater(GROUNDWATER_FILE, groundwater_id)

    
    # STEP 3 : PIPELINE SUMMARY

    print("\n========== PIPELINE EXECUTION SUMMARY ==========")

    total = 0
    for dataset_name, count in results.items():
        print(f"{dataset_name} → {count} signals inserted")
        total += count
    
    print(f"\nTOTAL SIGNALS INSERTED: {total}")

    logger.info(f"Total signals inserted: {total}")

    # STEP 4 — GENERATE REPORT

    print("\nGenerating pipeline execution report...")
    generate_pipeline_report(results, total)

    print("\n========== PIPELINE COMPLETED ==========")


if __name__ == "__main__":
    run_pipeline()
