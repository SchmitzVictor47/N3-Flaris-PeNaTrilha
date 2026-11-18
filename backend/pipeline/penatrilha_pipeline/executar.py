"""Executa o pipeline completo sem o Airflow (útil para depuração).

    python -m penatrilha_pipeline.executar            # pula se o dataset não mudou
    python -m penatrilha_pipeline.executar --forcar   # reprocessa mesmo assim
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timezone

from . import analises, camada_prata, captura, carga, transformacao, verificacao
from .spark import criar_sessao

log = logging.getLogger("penatrilha_pipeline")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline de ingestão do Pé na Trilha")
    parser.add_argument("--forcar", action="store_true", help="reprocessa mesmo sem mudança no dataset")
    parser.add_argument("--sem-analises", action="store_true", help="não gera o relatório de análises")
    argumentos = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    lote_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    inicio = time.monotonic()

    log.info("[1/8] Captura (lote %s)", lote_id)
    dados_captura = captura.capturar(lote_id)
    if not captura.ha_novidade(dados_captura["sha256"], argumentos.forcar):
        return

    spark = criar_sessao("cli", com_mongo=True)
    try:
        log.info("[2-4/8] Leitura, validação e limpeza → prata")
        camada_prata.executar(spark, lote_id, dados_captura)
        log.info("[5-6/8] Transformação e enriquecimento → ouro")
        transformacao.executar(spark, lote_id)
        log.info("[7/8] Carga no MongoDB")
        carga.preparar_mongodb(lote_id)
        carga.carregar(spark, lote_id)
    finally:
        spark.stop()

    log.info("[8/8] Verificação")
    resultado = verificacao.verificar(lote_id, dados_captura, origem="cli")
    if not argumentos.sem_analises:
        analises.gerar()
    log.info("Lote %s concluído em %.0f s: %s", lote_id, time.monotonic() - inicio, resultado["mongodb"])


if __name__ == "__main__":
    main()
