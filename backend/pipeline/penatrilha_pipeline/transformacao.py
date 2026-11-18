"""Etapa 5: transformação e enriquecimento → camada ouro (trilhas, trilheiros e percursos)."""

from __future__ import annotations

import logging
from typing import Iterator

import numpy as np
import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import armazenamento, config
from .gpx import extrair_pontos
from .textos import montar_descricao, montar_endereco

log = logging.getLogger(__name__)

ID_EXEMPLO = "5afb229e8f80884aaad9c6ea"  # primeiro registro do arquivo, usado como exemplo na N2

ESQUEMA_PONTOS = (
    "_id string, gpx_versao string, gpx_criador string, total_pontos int, "
    "inicio_lat double, inicio_lon double, "
    "pontos array<struct<t_ms:bigint,lat:double,lon:double,ele:double>>"
)
_COLUNAS_PONTOS = [
    "_id", "gpx_versao", "gpx_criador", "total_pontos", "inicio_lat", "inicio_lon", "pontos",
]

ESQUEMA_ENRIQUECIMENTO = (
    "_id string, cidade string, regiao string, pais string, pais_codigo string, "
    "endereco string, descricao string"
)
_COLUNAS_ENRIQUECIMENTO = [
    "_id", "cidade", "regiao", "pais", "pais_codigo", "endereco", "descricao",
]


def _pontos_lotes(lotes: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """gpxpy nos workers do Spark: GPX bruto → lista de pontos e ponto inicial."""
    for lote in lotes:
        registros = []
        for identificador, texto in zip(lote["_id"], lote["gpx"]):
            dados = extrair_pontos(texto)
            dados["_id"] = identificador
            registros.append(dados)
        yield pd.DataFrame(registros, columns=_COLUNAS_PONTOS)


def _enriquecer_lotes(lotes: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """reverse_geocoder (GeoNames, offline) para o endereço e modelo de texto para a descrição."""
    import reverse_geocoder
    from babel import Locale

    nomes_paises = Locale.parse("pt_BR").territories

    for lote in lotes:
        registros = lote.to_dict("records")
        locais: list[dict] = [{} for _ in registros]
        com_coordenada = [
            indice
            for indice, registro in enumerate(registros)
            if not (pd.isna(registro["inicio_lat"]) or pd.isna(registro["inicio_lon"]))
        ]
        if com_coordenada:
            consultas = [
                (float(registros[i]["inicio_lat"]), float(registros[i]["inicio_lon"]))
                for i in com_coordenada
            ]
            for indice, local in zip(
                com_coordenada, reverse_geocoder.search(consultas, mode=1, verbose=False)
            ):
                locais[indice] = local

        saida = []
        for registro, local in zip(registros, locais):
            codigo_pais = local.get("cc") or None
            pais = nomes_paises.get(codigo_pais, codigo_pais) if codigo_pais else None
            cidade = local.get("name") or None
            regiao = local.get("admin1") or None
            saida.append(
                {
                    "_id": registro["_id"],
                    "cidade": cidade,
                    "regiao": regiao,
                    "pais": pais,
                    "pais_codigo": codigo_pais,
                    "endereco": montar_endereco(cidade, regiao, pais),
                    "descricao": montar_descricao(
                        distancia_km=registro["distancia_km"],
                        subida_m=registro["subida_m"],
                        descida_m=registro["descida_m"],
                        altitude_min_m=registro["altitude_min_m"],
                        altitude_max_m=registro["altitude_max_m"],
                        dificuldade_codigo=registro["dificuldade_codigo"],
                        dificuldade_nome=registro["dificuldade_nome"],
                        tempo_movimento_min=registro["tempo_movimento_min"],
                        url=registro["url"],
                    ),
                }
            )
        yield pd.DataFrame(saida, columns=_COLUNAS_ENRIQUECIMENTO)


def _trilhas_ouro(trilhas: DataFrame) -> DataFrame:
    """Formato da coleção trilhas (N2, item 3), compatível com o model Trilha do app."""
    ponto_inicial = F.when(
        F.col("inicio_lat").isNotNull(),
        F.struct(
            F.lit("Point").alias("type"),
            F.array(F.col("inicio_lon"), F.col("inicio_lat")).alias("coordinates"),
        ),
    )
    return trilhas.select(
        "_id",
        "titulo",
        "descricao",
        "endereco",
        F.when(
            F.col("dificuldade_codigo").isNotNull(),
            F.struct(
                F.col("dificuldade_codigo").alias("codigo"),
                F.col("dificuldade_grau").alias("grau"),
                F.col("dificuldade_modificador").alias("modificador"),
                F.col("dificuldade_nome").alias("nome"),
            ),
        ).alias("dificuldade"),
        F.struct(
            ponto_inicial.alias("inicio"),
            F.col("cidade"),
            F.col("regiao"),
            F.col("pais"),
        ).alias("localizacao"),
        F.struct(
            "distancia_km",
            "subida_m",
            "descida_m",
            "altitude_min_m",
            "altitude_max_m",
            "tempo_movimento_min",
            "duracao_total_min",
            "velocidade_media_kmh",
            "velocidade_max_kmh",
        ).alias("metricas"),
        F.struct(
            F.col("periodo_inicio").alias("inicio"), F.col("periodo_fim").alias("fim")
        ).alias("periodo"),
        F.struct(
            F.concat(F.lit("hikr:"), F.col("usuario")).alias("_id"), F.col("usuario")
        ).alias("trilheiro"),
        F.struct("tem_tempo", "tem_elevacao", "alertas").alias("qualidade"),
        F.struct(
            F.lit(config.FONTE).alias("fonte"),
            F.lit(config.DATASET_KAGGLE).alias("dataset"),
            F.col("url"),
            F.col("post_id"),
        ).alias("origem"),
    )


def _trilheiros_ouro(trilhas: DataFrame) -> DataFrame:
    """Estatísticas pré-calculadas de cada trilheiro (recalculadas a cada carga)."""
    ordem_modificador = (
        F.when(F.col("dificuldade_modificador") == "-", 0)
        .when(F.col("dificuldade_modificador") == "+", 2)
        .otherwise(1)
    )
    return (
        trilhas.groupBy("usuario")
        .agg(
            F.count("*").alias("total_trilhas"),
            F.round(F.sum("distancia_m") / 1000, 2).alias("distancia_total_km"),
            F.round(F.sum("subida_bruta_m")).cast("long").alias("subida_total_m"),
            F.max_by("dificuldade_codigo", F.col("dificuldade_grau") * 10 + ordem_modificador).alias(
                "dificuldade_maxima"
            ),
            F.min("periodo_inicio").alias("primeira_trilha"),
            F.max("periodo_inicio").alias("ultima_trilha"),
        )
        .select(
            F.concat(F.lit("hikr:"), F.col("usuario")).alias("_id"),
            "usuario",
            F.lit(config.FONTE).alias("origem"),
            F.struct(
                "total_trilhas",
                "distancia_total_km",
                "subida_total_m",
                "dificuldade_maxima",
                "primeira_trilha",
                "ultima_trilha",
            ).alias("estatisticas"),
        )
    )


def executar(spark: SparkSession, lote_id: str) -> dict:
    prata = spark.read.parquet(str(config.PRATA_TRILHAS))

    # 5a. GPX → pontos GPS (lista ordenada) e ponto inicial em GeoJSON
    pontos = (
        prata.select("_id", "gpx")
        .repartition(config.PARTICOES_GPX)
        .mapInPandas(_pontos_lotes, ESQUEMA_PONTOS)
        .withColumn(
            "pontos",
            F.expr(
                "transform(pontos, p -> named_struct("
                "'t', timestamp_millis(p.t_ms), 'lat', p.lat, 'lon', p.lon, 'ele', p.ele))"
            ),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    pontos.count()

    # 5b. Endereço e descrição a partir do ponto inicial e das métricas
    base = prata.drop("gpx").join(pontos.select("_id", "inicio_lat", "inicio_lon"), on="_id")
    enriquecimento = base.select(
        "_id",
        "inicio_lat",
        "inicio_lon",
        "distancia_km",
        "subida_m",
        "descida_m",
        "altitude_min_m",
        "altitude_max_m",
        "dificuldade_codigo",
        "dificuldade_nome",
        "tempo_movimento_min",
        "url",
    ).mapInPandas(_enriquecer_lotes, ESQUEMA_ENRIQUECIMENTO)
    trilhas = base.join(enriquecimento, on="_id").persist(StorageLevel.MEMORY_AND_DISK)

    # 6. Camada ouro: três conjuntos já no formato das coleções do MongoDB
    armazenamento.gravar_parquet(_trilhas_ouro(trilhas), config.OURO_TRILHAS)
    armazenamento.gravar_parquet(_trilheiros_ouro(trilhas), config.OURO_TRILHEIROS)
    armazenamento.gravar_parquet(
        pontos.select(
            "_id",
            F.struct(F.col("gpx_versao").alias("versao"), F.col("gpx_criador").alias("criador")).alias("gpx"),
            "total_pontos",
            "pontos",
        ),
        config.OURO_PERCURSOS,
    )
    pontos.unpersist()
    trilhas.unpersist()

    # Resumo lido de volta da camada ouro
    ouro_trilhas = spark.read.parquet(str(config.OURO_TRILHAS))
    ouro_percursos = spark.read.parquet(str(config.OURO_PERCURSOS))
    ouro_trilheiros = spark.read.parquet(str(config.OURO_TRILHEIROS))
    pontos_resumo = ouro_percursos.agg(
        F.sum("total_pontos").alias("total"), F.max("total_pontos").alias("maximo")
    ).first()
    paises = (
        ouro_trilhas.groupBy(F.col("localizacao.pais").alias("pais"))
        .count()
        .orderBy(F.desc("count"))
        .limit(10)
        .collect()
    )
    exemplo = ouro_trilhas.filter(F.col("_id") == ID_EXEMPLO).first()
    trilheiro_exemplo = ouro_trilheiros.filter(F.col("_id") == "hikr:Bergfritz").first()

    relatorio = {
        "etapa": "transformacao_enriquecimento",
        "lote_id": lote_id,
        "trilhas": ouro_trilhas.count(),
        "trilheiros": ouro_trilheiros.count(),
        "percursos": ouro_percursos.count(),
        "pontos_gps": int(pontos_resumo["total"] or 0),
        "maior_percurso_pontos": int(pontos_resumo["maximo"] or 0),
        "trilhas_sem_ponto_inicial": ouro_trilhas.filter(F.col("localizacao.inicio").isNull()).count(),
        "paises_distintos": ouro_trilhas.select("localizacao.pais").distinct().count(),
        "top_paises": {linha["pais"]: linha["count"] for linha in paises},
        "exemplo_trilha": exemplo.asDict(recursive=True) if exemplo else None,
        "exemplo_trilheiro": trilheiro_exemplo.asDict(recursive=True) if trilheiro_exemplo else None,
        "camada_ouro": {
            "trilhas": str(config.OURO_TRILHAS),
            "trilheiros": str(config.OURO_TRILHEIROS),
            "percursos": str(config.OURO_PERCURSOS),
        },
    }
    armazenamento.salvar_relatorio_etapa(lote_id, "ouro", relatorio)
    log.info(
        "Ouro: %s trilhas, %s trilheiros, %s percursos, %s pontos GPS",
        relatorio["trilhas"], relatorio["trilheiros"], relatorio["percursos"], relatorio["pontos_gps"],
    )
    return relatorio
