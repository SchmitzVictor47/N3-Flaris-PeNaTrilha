"""
### Pipeline de ingestão do Pé na Trilha (Avaliação N3)

Implementa o fluxo especificado na N2: as 12.141 linhas do dataset
**GPS recorded hikes from hikr.org** (Kaggle) passam pelas camadas bronze, prata e ouro
e chegam ao MongoDB do app como 8.709 trilhas, 372 trilheiros e 8.709 percursos.

| Tarefa | Etapa da N2 |
| --- | --- |
| `captura` | 1 · Kaggle API → bronze (.zip intacto + SHA-256) |
| `verificar_novidade` | pula o restante se o hash não mudou (parâmetro `forcar` ignora) |
| `leitura_validacao_limpeza` | 2–4 · Spark → prata + quarentena (Parquet) |
| `transformacao_enriquecimento` | 5–6 · gpxpy + reverse_geocoder → ouro (Parquet) |
| `preparar_mongodb` | 7 · marca trilhas manuais e cria os índices |
| `carga_mongodb` | 7 · MongoDB Spark Connector, upsert por `_id` |
| `verificacao` | 8 · ouro × MongoDB e registro do lote |
| `analises` | análises do item f (relatórios/analises.md) |
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, ShortCircuitOperator


def _lote(context) -> str:
    return context["ts_nodash"]


def _captura(**context):
    from penatrilha_pipeline import captura

    return captura.capturar(_lote(context))


def _verificar_novidade(**context):
    from penatrilha_pipeline import captura

    dados = context["ti"].xcom_pull(task_ids="captura")
    return captura.ha_novidade(dados["sha256"], bool(context["params"].get("forcar")))


def _leitura_validacao_limpeza(**context):
    from penatrilha_pipeline import camada_prata
    from penatrilha_pipeline.spark import criar_sessao

    dados = context["ti"].xcom_pull(task_ids="captura")
    spark = criar_sessao("prata")
    try:
        relatorio = camada_prata.executar(spark, _lote(context), dados)
    finally:
        spark.stop()
    return {k: relatorio[k] for k in ("linhas_lidas", "trilhas_unicas", "quarentena_total", "trilhas_validas")}


def _transformacao_enriquecimento(**context):
    from penatrilha_pipeline import transformacao
    from penatrilha_pipeline.spark import criar_sessao

    spark = criar_sessao("ouro")
    try:
        relatorio = transformacao.executar(spark, _lote(context))
    finally:
        spark.stop()
    return {k: relatorio[k] for k in ("trilhas", "trilheiros", "percursos", "pontos_gps")}


def _preparar_mongodb(**context):
    from penatrilha_pipeline import carga

    return carga.preparar_mongodb(_lote(context))


def _carga_mongodb(**context):
    from penatrilha_pipeline import carga
    from penatrilha_pipeline.spark import criar_sessao

    spark = criar_sessao("carga", com_mongo=True)
    try:
        return carga.carregar(spark, _lote(context))
    finally:
        spark.stop()


def _verificacao(**context):
    from penatrilha_pipeline import verificacao

    dados = context["ti"].xcom_pull(task_ids="captura")
    resultado = verificacao.verificar(_lote(context), dados, origem=f"airflow:{context['run_id']}")
    return resultado["mongodb"] | {"status": resultado["status"]}


def _analises(**_context):
    from penatrilha_pipeline import analises

    return analises.gerar()


with DAG(
    dag_id="pe_na_trilha_ingestao",
    description="hikr.org (Kaggle) → bronze → prata → ouro → MongoDB do Pé na Trilha",
    doc_md=__doc__,
    schedule="@monthly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    params={
        "forcar": Param(
            False,
            type="boolean",
            title="Forçar reprocessamento",
            description="Processa e carrega de novo mesmo que o dataset não tenha mudado.",
        )
    },
    default_args={"owner": "pe-na-trilha", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["N3", "big-data", "spark", "mongodb"],
) as dag:
    captura = PythonOperator(task_id="captura", python_callable=_captura)
    verificar_novidade = ShortCircuitOperator(
        task_id="verificar_novidade", python_callable=_verificar_novidade
    )
    leitura_validacao_limpeza = PythonOperator(
        task_id="leitura_validacao_limpeza", python_callable=_leitura_validacao_limpeza
    )
    transformacao_enriquecimento = PythonOperator(
        task_id="transformacao_enriquecimento", python_callable=_transformacao_enriquecimento
    )
    preparar_mongodb = PythonOperator(task_id="preparar_mongodb", python_callable=_preparar_mongodb)
    carga_mongodb = PythonOperator(task_id="carga_mongodb", python_callable=_carga_mongodb)
    verificacao = PythonOperator(task_id="verificacao", python_callable=_verificacao, retries=0)
    analises = PythonOperator(task_id="analises", python_callable=_analises)

    (
        captura
        >> verificar_novidade
        >> leitura_validacao_limpeza
        >> transformacao_enriquecimento
        >> preparar_mongodb
        >> carga_mongodb
        >> verificacao
        >> analises
    )
