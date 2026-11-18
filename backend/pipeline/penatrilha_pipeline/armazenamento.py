"""Etapa 6: armazenamento em Parquet (Snappy) e relatórios de cada lote."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import DataFrame

from . import config

log = logging.getLogger(__name__)


def gravar_parquet(df: DataFrame, caminho: Path) -> None:
    """Grava (sobrescrevendo) um conjunto em Parquet comprimido com Snappy."""
    df.write.mode("overwrite").option("compression", "snappy").parquet(str(caminho))
    log.info("Parquet gravado em %s", caminho)


def _serializar(valor):
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()
    return str(valor)


def dir_lote(lote_id: str) -> Path:
    caminho = config.DIR_TRABALHO / "lotes" / lote_id
    caminho.mkdir(parents=True, exist_ok=True)
    return caminho


def salvar_relatorio_etapa(lote_id: str, etapa: str, conteudo: dict) -> Path:
    """Guarda o resumo de uma etapa para a verificação final do lote."""
    destino = dir_lote(lote_id) / f"{etapa}.json"
    destino.write_text(
        json.dumps(conteudo, ensure_ascii=False, indent=2, default=_serializar), encoding="utf-8"
    )
    return destino


def ler_relatorio_etapa(lote_id: str, etapa: str) -> dict:
    origem = dir_lote(lote_id) / f"{etapa}.json"
    if not origem.exists():
        return {}
    return json.loads(origem.read_text(encoding="utf-8"))


def salvar_json(caminho: Path, conteudo) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_text(
        json.dumps(conteudo, ensure_ascii=False, indent=2, default=_serializar), encoding="utf-8"
    )
