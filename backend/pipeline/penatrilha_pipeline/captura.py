"""Etapa 1: captura do dataset pela Kaggle API e gravação na camada bronze."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import requests

from . import config
from .mongo import conectar

log = logging.getLogger(__name__)
CABECALHOS = {"User-Agent": "pe-na-trilha-pipeline/1.0"}


def _sha256(caminho: Path) -> str:
    resumo = hashlib.sha256()
    with caminho.open("rb") as arquivo:
        for bloco in iter(lambda: arquivo.read(1024 * 1024), b""):
            resumo.update(bloco)
    return resumo.hexdigest()


def _tem_credenciais_kaggle() -> bool:
    if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
        return True
    return (Path.home() / ".kaggle" / "kaggle.json").exists()


def _baixar_com_cli(pasta: Path) -> Path:
    """kaggle datasets download -d roccoli/gpx-hike-tracks (exige token da Kaggle)."""
    subprocess.run(
        ["kaggle", "datasets", "download", "-d", config.DATASET_KAGGLE, "-p", str(pasta), "--force"],
        check=True,
    )
    return pasta / config.NOME_ZIP


def _baixar_com_http(destino: Path) -> Path:
    """Mesmo endpoint da Kaggle API, que é público para datasets abertos."""
    with requests.get(
        config.URL_KAGGLE_DOWNLOAD, headers=CABECALHOS, stream=True, timeout=120
    ) as resposta:
        resposta.raise_for_status()
        with destino.open("wb") as arquivo:
            for bloco in resposta.iter_content(chunk_size=1024 * 1024):
                arquivo.write(bloco)
    return destino


def _versao_dataset() -> dict:
    try:
        resposta = requests.get(config.URL_KAGGLE_METADADOS, headers=CABECALHOS, timeout=30)
        resposta.raise_for_status()
        dados = resposta.json()
        return {
            "versao": dados.get("currentVersionNumber"),
            "atualizado_em": dados.get("lastUpdated"),
            "licenca": dados.get("licenseName"),
        }
    except Exception as erro:  # a versão é informativa: a captura segue sem ela
        log.warning("Não foi possível consultar a versão do dataset: %s", erro)
        return {"versao": None, "atualizado_em": None, "licenca": None}


def capturar(lote_id: str) -> dict:
    """Baixa o .zip, calcula o SHA-256 e grava o arquivo intacto na camada bronze."""
    temporario = config.DIR_TRABALHO / f"download_{lote_id}"
    temporario.mkdir(parents=True, exist_ok=True)
    baixado = temporario / config.NOME_ZIP

    try:
        if _tem_credenciais_kaggle():
            baixado = _baixar_com_cli(temporario)
            metodo = "kaggle-cli"
        else:
            _baixar_com_http(baixado)
            metodo = "kaggle-api-http"
    except Exception as erro:
        # Sem internet: usa a cópia da pasta entrada/ ou o último .zip da camada bronze
        local = config.DIR_ENTRADA / config.NOME_ZIP
        anteriores = sorted(
            config.DIR_BRONZE.glob(f"kaggle/roccoli_gpx-hike-tracks/sha256=*/{config.NOME_ZIP}"),
            key=lambda caminho: caminho.stat().st_mtime,
            reverse=True,
        )
        if local.exists():
            origem, metodo = local, "arquivo-local"
        elif anteriores:
            origem, metodo = anteriores[0], "bronze-anterior"
        else:
            raise RuntimeError(
                f"Falha no download ({erro}) e não há cópia local em {local} nem captura anterior."
            ) from erro
        log.warning("Download falhou (%s); usando %s", erro, origem)
        shutil.copyfile(origem, baixado)

    sha256 = _sha256(baixado)
    pasta_bronze = config.DIR_BRONZE / "kaggle" / "roccoli_gpx-hike-tracks" / f"sha256={sha256[:16]}"
    destino = pasta_bronze / config.NOME_ZIP
    if destino.exists():
        log.info("Arquivo com o mesmo hash já está na camada bronze: %s", destino)
    else:
        pasta_bronze.mkdir(parents=True, exist_ok=True)
        shutil.move(str(baixado), destino)
    shutil.rmtree(temporario, ignore_errors=True)

    versao = _versao_dataset()
    captura = {
        "lote_id": lote_id,
        "dataset": config.DATASET_KAGGLE,
        "versao_dataset": versao["versao"],
        "dataset_atualizado_em": versao["atualizado_em"],
        "licenca": versao["licenca"],
        "metodo": metodo,
        "arquivo": str(destino),
        "tamanho_bytes": destino.stat().st_size,
        "sha256": sha256,
        "capturado_em": datetime.now(timezone.utc).isoformat(),
    }

    manifesto = pasta_bronze / "manifesto.json"
    historico = json.loads(manifesto.read_text(encoding="utf-8")) if manifesto.exists() else []
    historico.append(captura)
    manifesto.write_text(json.dumps(historico, ensure_ascii=False, indent=2), encoding="utf-8")

    log.info(
        "Captura concluída: %s (%s bytes, SHA-256 %s…, versão %s, via %s)",
        destino, captura["tamanho_bytes"], sha256[:16], versao["versao"], metodo,
    )
    return captura


def ha_novidade(sha256: str, forcar: bool = False) -> bool:
    """Se o hash é o mesmo do último lote concluído, o restante do fluxo é pulado."""
    if forcar:
        log.info("Reprocessamento forçado: o fluxo segue mesmo sem mudança no dataset.")
        return True
    cliente, banco = conectar()
    try:
        ultimo = banco.lotes.find_one({"status": "sucesso"}, sort=[("finalizado_em", -1)])
    finally:
        cliente.close()
    if ultimo and ultimo.get("sha256") == sha256:
        log.info(
            "Dataset sem mudanças desde o lote %s (SHA-256 %s…): etapas seguintes puladas.",
            ultimo["_id"], sha256[:16],
        )
        return False
    return True
