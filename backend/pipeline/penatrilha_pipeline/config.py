"""Configuração central: dataset, camadas do data lake, MongoDB e Spark."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlsplit

# Fonte de dados (N2, item 1.c)
DATASET_KAGGLE = "roccoli/gpx-hike-tracks"
URL_KAGGLE_DOWNLOAD = f"https://www.kaggle.com/api/v1/datasets/download/{DATASET_KAGGLE}"
URL_KAGGLE_METADADOS = f"https://www.kaggle.com/api/v1/datasets/view/{DATASET_KAGGLE}"
NOME_ZIP = "gpx-hike-tracks.zip"
NOME_CSV = "gpx-tracks-from-hikr.org.csv"
FONTE = "hikr.org"

# Camadas do data lake (N2, item 1.e)
DIR_DADOS = Path(os.getenv("PIPELINE_DIR_DADOS", "/opt/airflow/dados"))
DIR_BRONZE = DIR_DADOS / "bronze"
DIR_PRATA = DIR_DADOS / "prata"
DIR_OURO = DIR_DADOS / "ouro"
DIR_QUARENTENA = DIR_DADOS / "quarentena"
DIR_TRABALHO = DIR_DADOS / "trabalho"
DIR_ENTRADA = DIR_DADOS / "entrada"
DIR_RELATORIOS = Path(os.getenv("PIPELINE_DIR_RELATORIOS", "/opt/airflow/relatorios"))

PRATA_TRILHAS = DIR_PRATA / "trilhas"
QUARENTENA_TRILHAS = DIR_QUARENTENA / "trilhas"
OURO_TRILHAS = DIR_OURO / "trilhas"
OURO_TRILHEIROS = DIR_OURO / "trilheiros"
OURO_PERCURSOS = DIR_OURO / "percursos"


def _banco_da_uri(uri: str) -> str | None:
    """Extrai o nome do banco de uma URI (ex.: mongodb+srv://.../penatrilha)."""
    caminho = urlsplit(uri).path.lstrip("/")
    return caminho or None


# Destino (N2, item 1.g): o mesmo MongoDB do app
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB") or _banco_da_uri(MONGO_URI) or "penatrilha"

# Spark em modo local dentro do contêiner do Airflow
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "3g")
MONGO_SPARK_CONNECTOR = os.getenv(
    "MONGO_SPARK_CONNECTOR", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1"
)
SPARK_JARS_IVY = os.getenv("SPARK_JARS_IVY", "/opt/ivy")
PARTICOES_GPX = int(os.getenv("PIPELINE_PARTICOES_GPX", "8"))
