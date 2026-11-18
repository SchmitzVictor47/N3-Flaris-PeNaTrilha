"""Etapa 4: limpeza e padronização dos registros válidos (camada prata)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .referencias import (
    ANOS_INVALIDOS,
    ESCALA_SAC,
    SUBIDA_ALERTA_M,
    VELOCIDADE_MAXIMA_KMH,
    VELOCIDADE_MEDIA_ALERTA_KMH,
)

FORMATO_DATA = "yyyy-MM-dd HH:mm:ss"


def _texto_ou_nulo(coluna: str):
    valor = F.trim(F.col(coluna))
    return F.when(valor != "", valor)


def _sem_dado(coluna: str):
    """Zero que significa “sem dado”."""
    return F.coalesce(F.col(coluna), F.lit(0.0)) == 0


def padronizar(df: DataFrame, lote_id: str) -> DataFrame:
    # Datas em UTC; anos 0001 e 1970 e fim antes do início invalidam os tempos
    df = (
        df.withColumn("_inicio", F.to_timestamp(_texto_ou_nulo("start_time"), FORMATO_DATA))
        .withColumn("_fim", F.to_timestamp(_texto_ou_nulo("end_time"), FORMATO_DATA))
        .withColumn("sem_data_hora", F.col("_inicio").isNull() | F.col("_fim").isNull())
        .withColumn(
            "tempo_invalido",
            ~F.col("sem_data_hora")
            & (
                F.year("_inicio").isin(*ANOS_INVALIDOS)
                | F.year("_fim").isin(*ANOS_INVALIDOS)
                | (F.col("_fim") < F.col("_inicio"))
                | _sem_dado("moving_time")
            ),
        )
        .withColumn("tem_tempo", ~(F.col("sem_data_hora") | F.col("tempo_invalido")))
        # Sem elevação: subida e descida zeradas e GPX sem a tag <ele>
        .withColumn(
            "sem_elevacao",
            _sem_dado("uphill")
            & _sem_dado("downhill")
            & (F.coalesce(F.col("gpx_pontos_com_elevacao"), F.lit(0)) == 0),
        )
        .withColumn("tem_elevacao", ~F.col("sem_elevacao"))
    )

    # Unidades: m → km, s → min, m/s → km/h
    velocidade_media = (F.col("length_2d") / 1000) / (F.col("moving_time") / 3600)
    velocidade_maxima = F.col("max_speed") * 3.6
    elevacao = F.col("tem_elevacao")
    tempo = F.col("tem_tempo")
    df = (
        df.withColumn("distancia_km", F.round(F.col("length_2d") / 1000, 2))
        .withColumn("subida_m", F.when(elevacao, F.round("uphill").cast("int")))
        .withColumn("descida_m", F.when(elevacao, F.round("downhill").cast("int")))
        .withColumn("altitude_min_m", F.when(elevacao, F.round("min_elevation").cast("int")))
        .withColumn("altitude_max_m", F.when(elevacao, F.round("max_elevation").cast("int")))
        .withColumn("tempo_movimento_min", F.when(tempo, F.round(F.col("moving_time") / 60).cast("int")))
        .withColumn(
            "duracao_total_min",
            F.when(
                tempo,
                F.round((F.col("_fim").cast("long") - F.col("_inicio").cast("long")) / 60).cast("int"),
            ),
        )
        .withColumn("velocidade_media_kmh", F.when(tempo, F.round(velocidade_media, 2)))
        .withColumn(
            "velocidade_max_descartada",
            F.coalesce(velocidade_maxima > VELOCIDADE_MAXIMA_KMH, F.lit(False)),
        )
        .withColumn(
            "velocidade_max_kmh",
            F.when(tempo & ~F.col("velocidade_max_descartada"), F.round(velocidade_maxima, 2)),
        )
        .withColumn(
            "alertas",
            F.filter(
                F.array(
                    F.when(
                        tempo & (velocidade_media > VELOCIDADE_MEDIA_ALERTA_KMH),
                        F.lit("provavel_bike_ou_esqui"),
                    ),
                    F.when(F.col("uphill") > SUBIDA_ALERTA_M, F.lit("subida_atipica")),
                ),
                lambda alerta: alerta.isNotNull(),
            ),
        )
    )

    # Título: espaços das pontas removidos e espaços repetidos unificados
    nome = F.coalesce(F.col("name"), F.lit(""))
    titulo = F.regexp_replace(F.regexp_replace(nome, r"(?U)\s+", " "), r"^ | $", "")
    post_id = F.regexp_extract(F.col("url"), r"post(\d+)\.html", 1).cast("long")
    df = (
        df.withColumn("titulo", titulo)
        .withColumn("nome_ajustado", F.col("titulo") != nome)
        .withColumn("post_id", post_id)
        .withColumn(
            "titulo",
            F.when(F.col("titulo") != "", F.col("titulo")).otherwise(
                F.concat(F.lit("Relato hikr.org "), F.col("post_id").cast("string"))
            ),
        )
    )

    # Dificuldade: "T4- - High-level Alpine hike" → código T4-, grau 4, modificador "-"
    dificuldade = F.trim(F.coalesce(F.col("difficulty"), F.lit("")))
    codigo = F.regexp_extract(dificuldade, r"^(T[1-6][+-]?)", 1)
    grau = F.regexp_extract(dificuldade, r"^T([1-6])", 1)
    modificador = F.regexp_extract(dificuldade, r"^T[1-6]([+-]?)", 1)
    nomes_sac = F.create_map(*[F.lit(valor) for par in ESCALA_SAC.items() for valor in par])
    df = (
        df.withColumn("dificuldade_codigo", F.when(codigo != "", codigo))
        .withColumn("dificuldade_grau", F.when(grau != "", grau.cast("int")))
        .withColumn("dificuldade_modificador", F.when(modificador != "", modificador))
        .withColumn("dificuldade_nome", F.element_at(nomes_sac, F.col("dificuldade_grau")))
    )

    # length_3d (idêntico a length_2d) e bounds (mínimo e máximo invertidos) são descartados
    return df.select(
        "_id",
        "titulo",
        "nome_ajustado",
        F.trim(F.col("user")).alias("usuario"),
        "url",
        "post_id",
        "dificuldade_codigo",
        "dificuldade_grau",
        "dificuldade_modificador",
        "dificuldade_nome",
        F.when(tempo, F.col("_inicio")).alias("periodo_inicio"),
        F.when(tempo, F.col("_fim")).alias("periodo_fim"),
        F.col("length_2d").alias("distancia_m"),
        "distancia_km",
        F.col("uphill").alias("subida_bruta_m"),
        "subida_m",
        "descida_m",
        "altitude_min_m",
        "altitude_max_m",
        "tempo_movimento_min",
        "duracao_total_min",
        "velocidade_media_kmh",
        "velocidade_max_kmh",
        "tem_tempo",
        "tem_elevacao",
        "sem_data_hora",
        "tempo_invalido",
        "sem_elevacao",
        "velocidade_max_descartada",
        "alertas",
        "gpx",
        "gpx_versao",
        "gpx_criador",
        "gpx_total_pontos",
        F.lit(lote_id).alias("lote_id"),
        F.current_timestamp().alias("processado_em"),
    )
