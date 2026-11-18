"""Criação da SparkSession usada pelas etapas 2 a 7."""

from __future__ import annotations

import glob
import os

from pyspark.sql import SparkSession

from . import config


def criar_sessao(nome: str, com_mongo: bool = False) -> SparkSession:
    construtor = (
        SparkSession.builder.appName(f"pe-na-trilha-{nome}")
        .master(config.SPARK_MASTER)
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Cada linha carrega um GPX inteiro: lotes pequenos para o mapInPandas
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "200")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if com_mongo:
        jars = sorted(glob.glob(os.path.join(config.SPARK_JARS_IVY, "jars", "*.jar")))
        if jars:
            construtor = construtor.config("spark.jars", ",".join(jars))
        else:
            construtor = construtor.config(
                "spark.jars.packages", config.MONGO_SPARK_CONNECTOR
            ).config("spark.jars.ivy", config.SPARK_JARS_IVY)

    sessao = construtor.getOrCreate()
    # O conector repete avisos inofensivos (CaseInsensitiveStringMap) a cada gravação
    sessao.sparkContext.setLogLevel("ERROR" if com_mongo else "WARN")
    return sessao
