"""Etapas 2 a 4 em um job Spark: leitura, validação e limpeza → camadas prata e quarentena."""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import armazenamento, config, leitura, limpeza, validacao
from .referencias import MOTIVOS_QUARENTENA

log = logging.getLogger(__name__)


def executar(spark: SparkSession, lote_id: str, captura: dict) -> dict:
    # 2. Leitura
    csv = leitura.extrair_csv(Path(captura["arquivo"]), captura["sha256"])
    bruto = leitura.ler_csv(spark, csv).persist(StorageLevel.MEMORY_AND_DISK)
    linhas_lidas = bruto.count()
    linhas_malformadas = bruto.filter(F.col(leitura.COLUNA_CORROMPIDA).isNotNull()).count()
    length_3d_diferente = bruto.filter(~F.col("length_3d").eqNullSafe(F.col("length_2d"))).count()
    log.info("Leitura: %s linhas (%s malformadas)", linhas_lidas, linhas_malformadas)

    # 3. Validação: deduplicação por url, leitura do GPX e quarentena
    unicas = validacao.remover_duplicatas(bruto).persist(StorageLevel.MEMORY_AND_DISK)
    trilhas_unicas = unicas.count()
    bruto.unpersist()
    log.info("Deduplicação: %s trilhas únicas (%s duplicatas)", trilhas_unicas, linhas_lidas - trilhas_unicas)

    status = validacao.status_gpx(unicas).persist(StorageLevel.MEMORY_AND_DISK)
    status.count()
    avaliadas = unicas.join(F.broadcast(status), on="_id", how="left").withColumn(
        "motivo_quarentena", validacao.motivo_quarentena()
    )

    descricoes = F.create_map(*[F.lit(valor) for par in MOTIVOS_QUARENTENA.items() for valor in par])
    destino_quarentena = config.QUARENTENA_TRILHAS / f"lote_id={lote_id}"
    quarentena = avaliadas.filter(F.col("motivo_quarentena").isNotNull()).select(
        *leitura.COLUNAS_CSV,
        "motivo_quarentena",
        F.element_at(descricoes, F.col("motivo_quarentena")).alias("descricao_motivo"),
        F.current_timestamp().alias("quarentenado_em"),
    )
    armazenamento.gravar_parquet(quarentena, destino_quarentena)

    # 4. Limpeza e padronização → camada prata
    validas = avaliadas.filter(F.col("motivo_quarentena").isNull())
    armazenamento.gravar_parquet(limpeza.padronizar(validas, lote_id), config.PRATA_TRILHAS)
    unicas.unpersist()
    status.unpersist()

    # Estatísticas lidas de volta do Parquet (colunar: o GPX não é relido)
    por_motivo = {
        linha["motivo_quarentena"]: linha["count"]
        for linha in spark.read.parquet(str(destino_quarentena))
        .groupBy("motivo_quarentena")
        .count()
        .collect()
    }
    soma = lambda coluna: F.sum(F.col(coluna).cast("int"))  # noqa: E731
    prata = spark.read.parquet(str(config.PRATA_TRILHAS))
    resumo = prata.agg(
        F.count("*").alias("trilhas_validas"),
        soma("sem_data_hora").alias("sem_data_hora"),
        soma("tempo_invalido").alias("tempo_invalido"),
        soma("sem_elevacao").alias("sem_elevacao"),
        soma("velocidade_max_descartada").alias("velocidade_max_descartada"),
        soma("nome_ajustado").alias("nome_ajustado"),
        F.sum(F.array_contains("alertas", "provavel_bike_ou_esqui").cast("int")).alias("provavel_bike_ou_esqui"),
        F.sum(F.array_contains("alertas", "subida_atipica").cast("int")).alias("subida_atipica"),
        F.sum((F.col("tem_tempo") & F.col("tem_elevacao")).cast("int")).alias("trilhas_completas"),
        F.countDistinct("usuario").alias("trilheiros"),
    ).first().asDict()

    relatorio = {
        "etapa": "leitura_validacao_limpeza",
        "lote_id": lote_id,
        "arquivo_csv": str(csv),
        "linhas_lidas": linhas_lidas,
        "linhas_malformadas": linhas_malformadas,
        "length_3d_igual_length_2d": length_3d_diferente == 0,
        "duplicatas_removidas": linhas_lidas - trilhas_unicas,
        "trilhas_unicas": trilhas_unicas,
        "quarentena": dict(sorted(por_motivo.items(), key=lambda item: -item[1])),
        "quarentena_total": sum(por_motivo.values()),
        **resumo,
        "colunas_descartadas": ["length_3d", "bounds"],
        "camada_prata": str(config.PRATA_TRILHAS),
        "camada_quarentena": str(destino_quarentena),
    }
    armazenamento.salvar_relatorio_etapa(lote_id, "prata", relatorio)
    log.info(
        "Prata: %s trilhas válidas | quarentena: %s %s",
        resumo["trilhas_validas"], relatorio["quarentena_total"], relatorio["quarentena"],
    )
    return relatorio
