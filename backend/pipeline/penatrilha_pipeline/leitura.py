"""Etapa 2: leitura do CSV com o Apache Spark."""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from . import config

log = logging.getLogger(__name__)

COLUNA_CORROMPIDA = "_registro_corrompido"

# Esquema explícito, na ordem das 17 colunas do arquivo (N2, item 1.d).
# Datas entram como texto para que anos 0001 e 1970 sejam tratados na limpeza.
ESQUEMA_CSV = StructType(
    [
        StructField("_id", StringType()),
        StructField("length_3d", DoubleType()),
        StructField("user", StringType()),
        StructField("start_time", StringType()),
        StructField("max_elevation", DoubleType()),
        StructField("bounds", StringType()),
        StructField("uphill", DoubleType()),
        StructField("moving_time", DoubleType()),
        StructField("end_time", StringType()),
        StructField("max_speed", DoubleType()),
        StructField("gpx", StringType()),
        StructField("difficulty", StringType()),
        StructField("min_elevation", DoubleType()),
        StructField("url", StringType()),
        StructField("downhill", DoubleType()),
        StructField("name", StringType()),
        StructField("length_2d", DoubleType()),
        StructField(COLUNA_CORROMPIDA, StringType()),
    ]
)
COLUNAS_CSV = [campo.name for campo in ESQUEMA_CSV.fields if campo.name != COLUNA_CORROMPIDA]


def extrair_csv(arquivo_zip: Path, sha256: str) -> Path:
    """Descompacta o CSV do .zip da camada bronze numa área de trabalho (o .zip fica intacto)."""
    pasta = config.DIR_TRABALHO / f"sha256={sha256[:16]}"
    destino = pasta / config.NOME_CSV
    with zipfile.ZipFile(arquivo_zip) as pacote:
        tamanho = pacote.getinfo(config.NOME_CSV).file_size
        if destino.exists() and destino.stat().st_size == tamanho:
            log.info("CSV já extraído em %s", destino)
            return destino
        pasta.mkdir(parents=True, exist_ok=True)
        pacote.extract(config.NOME_CSV, pasta)
    log.info("CSV extraído em %s (%s bytes)", destino, tamanho)
    return destino


def ler_csv(spark: SparkSession, caminho_csv: Path) -> DataFrame:
    """multiLine e escape='"' são necessários: o GPX ocupa centenas de linhas e usa aspas duplicadas."""
    return (
        spark.read.schema(ESQUEMA_CSV)
        .option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", COLUNA_CORROMPIDA)
        .csv(str(caminho_csv))
    )
