"""Leitura do GPX embutido no CSV com a biblioteca gpxpy.

Funções puras (sem Spark), chamadas dentro do Spark por mapInPandas e
cobertas pelos testes em tests/test_gpx.py.
"""

from __future__ import annotations

import xml.etree.ElementTree as ElementTree
from datetime import datetime, timezone
from typing import Iterator

import gpxpy
import gpxpy.parser
from gpxpy.gpx import GPX, GPXTrackPoint

from .referencias import ANOS_INVALIDOS

# Com lxml instalado o gpxpy passa a usá-lo, mas para estes arquivos o ElementTree
# da biblioteca padrão é cerca de 3,5x mais rápido (6,8 ms × 24 ms por GPX).
gpxpy.parser.mod_etree = ElementTree


def _pontos_gravados(gpx: GPX) -> Iterator[GPXTrackPoint]:
    """Percorre gpx → trk → trkseg → trkpt, na ordem de gravação."""
    for trilha in gpx.tracks:
        for segmento in trilha.segments:
            yield from segmento.points


def analisar_gpx(texto: str | None) -> dict:
    """Valida o GPX (etapa 3): XML legível e existência de trilha gravada.

    Rotas planejadas (rte) não contam como trilha gravada.
    """
    resultado = {
        "gpx_status": "ok",
        "gpx_versao": None,
        "gpx_criador": None,
        "gpx_total_pontos": 0,
        "gpx_pontos_com_elevacao": 0,
    }
    if texto is None or not texto.strip():
        resultado["gpx_status"] = "sem_trilha"
        return resultado

    try:
        gpx = gpxpy.parse(texto)
    except Exception:  # gpxpy levanta exceções diferentes para XML e valores inválidos
        resultado["gpx_status"] = "xml_invalido"
        return resultado

    total = com_elevacao = 0
    for ponto in _pontos_gravados(gpx):
        total += 1
        if ponto.elevation is not None:
            com_elevacao += 1

    resultado.update(
        gpx_versao=gpx.version,
        gpx_criador=gpx.creator,
        gpx_total_pontos=total,
        gpx_pontos_com_elevacao=com_elevacao,
    )
    if total == 0:
        resultado["gpx_status"] = "sem_trilha"
    return resultado


def _epoch_ms(momento: datetime | None) -> int | None:
    """Converte a hora do ponto para milissegundos UTC; anos 0001 e 1970 viram nulo."""
    if momento is None or momento.year in ANOS_INVALIDOS:
        return None
    if momento.tzinfo is None:
        momento = momento.replace(tzinfo=timezone.utc)
    return int(momento.timestamp() * 1000)


def _coordenada_valida(lat: float | None, lon: float | None) -> bool:
    return lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180


def extrair_pontos(texto: str) -> dict:
    """Extrai os pontos GPS e o ponto inicial da trilha (etapa 5)."""
    gpx = gpxpy.parse(texto)
    pontos = []
    inicio = None
    for ponto in _pontos_gravados(gpx):
        lat, lon = ponto.latitude, ponto.longitude
        pontos.append(
            {"t_ms": _epoch_ms(ponto.time), "lat": lat, "lon": lon, "ele": ponto.elevation}
        )
        if inicio is None and _coordenada_valida(lat, lon):
            inicio = (lat, lon)

    return {
        "gpx_versao": gpx.version,
        "gpx_criador": gpx.creator,
        "total_pontos": len(pontos),
        "inicio_lat": inicio[0] if inicio else None,
        "inicio_lon": inicio[1] if inicio else None,
        "pontos": pontos,
    }
