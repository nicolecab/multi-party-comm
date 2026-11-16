#Registro simples de métricas locais de cada processo.
import csv
from pathlib import Path

_METRICS_FILE = "metrics.csv"

def set_metrics_file(filename):
    #Define o nome do CSV usado por esta implementação neste processo.
    global _METRICS_FILE
    _METRICS_FILE = filename

def register_metric(operation, duration, success=True):
    #Acrescenta uma medição em milissegundos ao CSV local.
    file_path = Path(__file__).resolve().parent / _METRICS_FILE
    exists = file_path.exists()

    with file_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["Operation", "Duration_ms", "Success"])
        writer.writerow([operation, round(duration * 1000, 3), success])
