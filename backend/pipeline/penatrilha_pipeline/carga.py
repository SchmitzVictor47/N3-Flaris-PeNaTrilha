"""Etapa 7: índices e carga com upsert por _id no MongoDB (MongoDB Spark Connector)."""

from __future__ import annotations

import logging

from pymongo import ASCENDING, GEOSPHERE, TEXT
from pymongo.errors import OperationFailure
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import armazenamento, config
from .mongo import conectar

log = logging.getLogger(__name__)

# Índices definidos na N2 (item 1.e, passo 7)
INDICES_TRILHAS = [
    ([("localizacao.inicio", GEOSPHERE)], {"name": "localizacao_inicio_2dsphere"}),
    (
        [("dificuldade.grau", ASCENDING), ("metricas.distancia_km", ASCENDING)],
        {"name": "dificuldade_grau_distancia_km"},
    ),
    ([("trilheiro._id", ASCENDING)], {"name": "trilheiro_id"}),
    ([("titulo", TEXT)], {"name": "titulo_texto", "default_language": "none"}),
]


def preparar_mongodb(lote_id: str) -> dict:
    """Marca as trilhas cadastradas pelo app e cria os índices antes da carga."""
    cliente, banco = conectar()
    try:
        marcadas = banco.trilhas.update_many(
            {"origem.fonte": {"$exists": False}}, {"$set": {"origem.fonte": "manual"}}
        ).modified_count

        criados, avisos = [], []
        for chaves, opcoes in INDICES_TRILHAS:
            try:
                criados.append(banco.trilhas.create_index(chaves, **opcoes))
            except OperationFailure as erro:  # ex.: já existe um índice de texto com outro nome
                avisos.append(f"{opcoes['name']}: {erro.details.get('errmsg', erro)}")
                log.warning("Índice %s não criado: %s", opcoes["name"], erro)

        resumo = {
            "etapa": "preparar_mongodb",
            "lote_id": lote_id,
            "banco": config.MONGO_DB,
            "trilhas_manuais_marcadas": marcadas,
            "trilhas_manuais_total": banco.trilhas.count_documents({"origem.fonte": "manual"}),
            "indices_criados": criados,
            "avisos": avisos,
        }
    finally:
        cliente.close()

    armazenamento.salvar_relatorio_etapa(lote_id, "preparar_mongodb", resumo)
    log.info("MongoDB preparado: %s", resumo)
    return resumo


def _ids_manuais() -> list[str]:
    cliente, banco = conectar()
    try:
        return [str(doc["_id"]) for doc in banco.trilhas.find({"origem.fonte": "manual"}, {"_id": 1})]
    finally:
        cliente.close()


def _criacoes_existentes(spark: SparkSession) -> DataFrame:
    """createdAt das trilhas já importadas: a recarga mantém a data da primeira carga."""
    cliente, banco = conectar()
    try:
        linhas = [
            (str(doc["_id"]), doc["createdAt"])
            for doc in banco.trilhas.find(
                {"origem.fonte": config.FONTE, "createdAt": {"$exists": True}}, {"createdAt": 1}
            )
        ]
    finally:
        cliente.close()
    return spark.createDataFrame(linhas, "_id string, createdAt_existente timestamp")


def _como_object_id(df: DataFrame) -> DataFrame:
    """'5afb…' → '{"$oid": "5afb…"}'; com convertJson o conector grava um ObjectId de verdade."""
    return df.withColumn("_id", F.to_json(F.struct(F.col("_id").alias("$oid"))))


def _gravar(df: DataFrame, colecao: str) -> None:
    (
        df.write.format("mongodb")
        .mode("append")
        .option("connection.uri", config.MONGO_URI)
        .option("database", config.MONGO_DB)
        .option("collection", colecao)
        .option("operationType", "replace")
        .option("idFieldList", "_id")
        .option("upsertDocument", "true")
        .option("convertJson", "objectOrArrayOnly")
        .option("maxBatchSize", "256")
        .save()
    )
    log.info("Coleção %s gravada (upsert por _id)", colecao)


def carregar(spark: SparkSession, lote_id: str) -> dict:
    manuais = _ids_manuais()
    trilhas = spark.read.parquet(str(config.OURO_TRILHAS))
    percursos = spark.read.parquet(str(config.OURO_PERCURSOS))
    trilheiros = spark.read.parquet(str(config.OURO_TRILHEIROS))

    if manuais:  # trilhas cadastradas pelo app nunca são sobrescritas
        trilhas = trilhas.filter(~F.col("_id").isin(manuais))
        percursos = percursos.filter(~F.col("_id").isin(manuais))

    agora = F.current_timestamp()
    trilhas = (
        trilhas.join(F.broadcast(_criacoes_existentes(spark)), on="_id", how="left")
        .withColumn("createdAt", F.coalesce(F.col("createdAt_existente"), agora))
        .withColumn("updatedAt", agora)
        .drop("createdAt_existente")
    )

    contagens = {
        "trilhas": trilhas.count(),
        "percursos": percursos.count(),
        "trilheiros": trilheiros.count(),
    }
    _gravar(_como_object_id(trilhas), "trilhas")
    _gravar(_como_object_id(percursos), "percursos")
    _gravar(trilheiros, "trilheiros")

    resumo = {
        "etapa": "carga_mongodb",
        "lote_id": lote_id,
        "banco": config.MONGO_DB,
        "conector": config.MONGO_SPARK_CONNECTOR,
        "operacao": "replace com upsert por _id",
        "ids_manuais_protegidos": len(manuais),
        "documentos_enviados": contagens,
    }
    armazenamento.salvar_relatorio_etapa(lote_id, "carga", resumo)
    log.info("Carga concluída: %s", contagens)
    return resumo
