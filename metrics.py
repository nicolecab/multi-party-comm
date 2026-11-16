import csv
import os
import time

def register_metric(operation, duration, success=True):
    file = "metrics.csv"
    exists = os.path.isfile(file)

    with open(file, "a", newline="") as f:
        writer = csv.writer(f)

        if not exists:
            writer.writerow([
                "Operation",
                "Duration_ms",
                "Success"
            ])

        writer.writerow([
            operation,
            round(duration * 1000, 3),
            success
        ])