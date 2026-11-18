"""Etapa 3: validação — deduplicação, leitura do GPX e regras de quarentena."""

from __future__ import annotations

from typing import Iterator

import pandas as pd
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from . import config
from .gpx import analisar_gpx
from .leitura import COLUNA_CORROMPIDA
from .referencias import DISTANCIA_MAXIMA_M, DISTANCIA_MINIMA_M

ESQUEMA_STATUS_GPX = (
    "_id string, gpx_status string, gpx_versao string, gpx_criador string, "
    "gpx_total_pontos int, gpx_pontos_com_elevacao int"
)
_COLUNAS_STATUS = [
    "gpx_status", "gpx_versao", "gpx_criador", "gpx_total_pontos", "gpx_pontos_com_elevacao",
]


def remover_duplicatas(df: DataFrame) -> DataFrame:
    """Mesma url = mesmo relato. Fica o registro de menor _id."""
    janela = Window.partitionBy(F.coalesce(F.col("url"), F.col("_id"))).orderBy(F.col("_id").asc())
    return df.withColumn("_ordem", F.row_number().over(janela)).filter("_ordem = 1").drop("_ordem")


def _status_gpx_lotes(lotes: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Roda nos workers Python do Spark: um lote de linhas por vez."""
    for lote in lotes:
        linhas = [analisar_gpx(texto) for texto in lote["gpx"]]
        saida = pd.DataFrame(linhas, columns=_COLUNAS_STATUS)
        saida.insert(0, "_id", lote["_id"].to_numpy())
        yield saida


def status_gpx(df: DataFrame) -> DataFrame:
    """gpxpy dentro do Spark (mapInPandas), em paralelo por partição."""
    return (
        df.select("_id", "gpx")
        .repartition(config.PARTICOES_GPX)
        .mapInPandas(_status_gpx_lotes, ESQUEMA_STATUS_GPX)
        .dropDuplicates(["_id"])
    )


def motivo_quarentena() -> Column:
    """Erros irrecuperáveis, avaliados nesta ordem; nulo = registro segue para a limpeza."""
    distancia = F.col("length_2d")
    return (
        F.when(F.col(COLUNA_CORROMPIDA).isNotNull(), F.lit("linha_malformada"))
        .when(
            F.col("_id").isNull() | ~F.col("_id").rlike("^[0-9a-fA-F]{24}$"), F.lit("id_invalido")
        )
        .when(F.col("gpx_status") == "xml_invalido", F.lit("gpx_xml_invalido"))
        .when(F.col("gpx_status") == "sem_trilha", F.lit("gpx_sem_trilha_gravada"))
        .when(distancia.isNull(), F.lit("distancia_ausente"))
        .when(distancia > DISTANCIA_MAXIMA_M, F.lit("distancia_acima_100km"))
        .when(distancia < DISTANCIA_MINIMA_M, F.lit("distancia_abaixo_0_5km"))
    )
