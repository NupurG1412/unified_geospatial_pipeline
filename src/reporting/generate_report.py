import os
from datetime import datetime


def generate_pipeline_report(results, total):

    os.makedirs("reports", exist_ok=True)

    report_path = "reports/pipeline_report.txt"

    with open(report_path, "w") as f:

        f.write("UNIFIED GEOSPATIAL SIGNAL PIPELINE REPORT\n")
        f.write("="*50 + "\n")

        f.write(f"Execution Time: {datetime.now()}\n\n")

        for dataset, count in results.items():
            f.write(f"{dataset}: {count} signals inserted\n")

        f.write("\n")
        f.write(f"TOTAL SIGNALS INSERTED: {total}\n")

    print(f"Report generated: {report_path}")