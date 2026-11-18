"""Regras de validação e limpeza da N2 aplicadas pelo Spark em registros sintéticos."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from penatrilha_pipeline import limpeza, validacao
from penatrilha_pipeline.leitura import ESQUEMA_CSV

BASE = {
    "_id": "5afb229e8f80884aaad9c6ea",
    "length_3d": 10832.95,
    "user": "Bergfritz",
    "start_time": "2018-05-11 07:37:40",
    "max_elevation": 1934.47,
    "bounds": None,
    "uphill": 612.88,
    "moving_time": 12155.0,
    "end_time": "2018-05-11 11:38:23",
    "max_speed": 1.5954933648455245,
    "gpx": "<gpx/>",
    "difficulty": "T2 - Mountain hike",
    "min_elevation": 1322.96,
    "url": "http://www.hikr.org/tour/post131855.html",
    "downhill": 609.67,
    "name": "Remsteinkopf, 1945 m",
    "length_2d": 10832.95,
    "_registro_corrompido": None,
}


@pytest.fixture(scope="module")
def spark():
    sessao = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield sessao
    sessao.stop()


def _processar(spark, *alteracoes, status="ok", pontos_com_elevacao=100):
    linhas = [{**BASE, "_id": f"{i:024x}", **alteracao} for i, alteracao in enumerate(alteracoes)]
    df = (
        spark.createDataFrame(linhas, ESQUEMA_CSV)
        .withColumn("gpx_status", F.lit(status))
        .withColumn("gpx_versao", F.lit("1.1"))
        .withColumn("gpx_criador", F.lit("teste"))
        .withColumn("gpx_total_pontos", F.lit(100))
        .withColumn("gpx_pontos_com_elevacao", F.lit(pontos_com_elevacao))
        .withColumn("motivo_quarentena", validacao.motivo_quarentena())
    )
    return df


def test_quarentena_por_distancia_e_gpx(spark):
    df = _processar(spark, {}, {"length_2d": 150_000.0}, {"length_2d": 300.0})
    motivos = [linha["motivo_quarentena"] for linha in df.orderBy("_id").collect()]
    assert motivos == [None, "distancia_acima_100km", "distancia_abaixo_0_5km"]
    assert _processar(spark, {}, status="sem_trilha").first()["motivo_quarentena"] == "gpx_sem_trilha_gravada"
    assert _processar(spark, {}, status="xml_invalido").first()["motivo_quarentena"] == "gpx_xml_invalido"


def test_metricas_do_exemplo_da_n2(spark):
    linha = limpeza.padronizar(_processar(spark, {}), "teste").first()
    assert linha["titulo"] == "Remsteinkopf, 1945 m"
    assert (linha["dificuldade_codigo"], linha["dificuldade_grau"], linha["dificuldade_modificador"]) == ("T2", 2, None)
    assert linha["dificuldade_nome"] == "Caminhada de montanha"
    assert linha["distancia_km"] == 10.83
    assert (linha["subida_m"], linha["descida_m"]) == (613, 610)
    assert (linha["altitude_min_m"], linha["altitude_max_m"]) == (1323, 1934)
    assert (linha["tempo_movimento_min"], linha["duracao_total_min"]) == (203, 241)
    assert (linha["velocidade_media_kmh"], linha["velocidade_max_kmh"]) == (3.21, 5.74)
    assert linha["post_id"] == 131855
    assert linha["tem_tempo"] and linha["tem_elevacao"]
    assert linha["alertas"] == []


def test_regras_de_tempo(spark):
    df = limpeza.padronizar(
        _processar(
            spark,
            {"start_time": "", "end_time": "", "moving_time": 0.0},
            {"start_time": "1970-01-01 00:00:00"},
            {"end_time": "2018-05-11 06:00:00"},
            {"moving_time": 0.0},
        ),
        "teste",
    ).orderBy("_id")
    linhas = df.collect()
    assert linhas[0]["sem_data_hora"] and not linhas[0]["tempo_invalido"]
    assert all(linha["tempo_invalido"] for linha in linhas[1:])
    assert all(not linha["tem_tempo"] for linha in linhas)
    assert all(linha["periodo_inicio"] is None and linha["tempo_movimento_min"] is None for linha in linhas)


def test_sem_elevacao_exige_gpx_sem_tag_ele(spark):
    zerado = {"uphill": 0.0, "downhill": 0.0}
    sem_tag = limpeza.padronizar(_processar(spark, zerado, pontos_com_elevacao=0), "t").first()
    com_tag = limpeza.padronizar(_processar(spark, zerado, pontos_com_elevacao=50), "t").first()
    assert not sem_tag["tem_elevacao"] and sem_tag["subida_m"] is None
    assert com_tag["tem_elevacao"]


def test_alertas_velocidade_e_nome(spark):
    linha = limpeza.padronizar(
        _processar(
            spark,
            {"moving_time": 3600.0, "uphill": 5200.0, "max_speed": 15.0, "name": "  Piz   Buin  "},
        ),
        "t",
    ).first()
    assert sorted(linha["alertas"]) == ["provavel_bike_ou_esqui", "subida_atipica"]
    assert linha["velocidade_max_descartada"] and linha["velocidade_max_kmh"] is None
    assert linha["titulo"] == "Piz Buin" and linha["nome_ajustado"]


def test_dificuldade_com_modificador(spark):
    linha = limpeza.padronizar(
        _processar(spark, {"difficulty": "T4- - High-level Alpine hike"}), "t"
    ).first()
    assert (linha["dificuldade_codigo"], linha["dificuldade_grau"], linha["dificuldade_modificador"]) == ("T4-", 4, "-")
    assert linha["dificuldade_nome"] == "Caminhada alpina"


def test_deduplicacao_mantem_menor_id(spark):
    linhas = [
        {**BASE, "_id": "5afb229e8f80884aaad9c6eb"},
        {**BASE, "_id": "5afb229e8f80884aaad9c6ea"},
        {**BASE, "_id": "5afb229e8f80884aaad9c6ec", "url": "http://www.hikr.org/tour/post1.html"},
    ]
    unicas = validacao.remover_duplicatas(spark.createDataFrame(linhas, ESQUEMA_CSV))
    assert sorted(linha["_id"] for linha in unicas.collect()) == [
        "5afb229e8f80884aaad9c6ea",
        "5afb229e8f80884aaad9c6ec",
    ]
