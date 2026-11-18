"""Montagem dos textos da trilha no padrão brasileiro (descrição e endereço)."""

from __future__ import annotations

import math


def vazio(valor) -> bool:
    """None, NaN do pandas ou texto em branco."""
    if valor is None:
        return True
    if isinstance(valor, float) and math.isnan(valor):
        return True
    return isinstance(valor, str) and not valor.strip()


def formatar_numero(valor: float, casas: int = 0) -> str:
    """1323 → '1.323'; 10.832 → '10,83'."""
    texto = f"{valor:,.{casas}f}"
    return texto.translate(str.maketrans({",": ".", ".": ","}))


def formatar_duracao(minutos: float) -> str:
    """203 → '3 h 23 min'; 45 → '45 min'; 120 → '2 h'."""
    horas, resto = divmod(int(round(minutos)), 60)
    if horas and resto:
        return f"{horas} h {resto} min"
    if horas:
        return f"{horas} h"
    return f"{resto} min"


def montar_descricao(
    distancia_km,
    subida_m=None,
    descida_m=None,
    altitude_min_m=None,
    altitude_max_m=None,
    dificuldade_codigo=None,
    dificuldade_nome=None,
    tempo_movimento_min=None,
    url=None,
) -> str:
    """Modelo de texto da N2; trechos sem dado são omitidos."""
    trecho = f"Trilha de {formatar_numero(distancia_km, 2)} km"
    if not vazio(subida_m) and not vazio(descida_m):
        trecho += (
            f" com {formatar_numero(subida_m)} m de subida"
            f" e {formatar_numero(descida_m)} m de descida"
        )
        if not vazio(altitude_min_m) and not vazio(altitude_max_m):
            trecho += (
                f", entre {formatar_numero(altitude_min_m)} m"
                f" e {formatar_numero(altitude_max_m)} m de altitude"
            )
    frases = [trecho + "."]

    if not vazio(dificuldade_codigo):
        if vazio(dificuldade_nome):
            frases.append(f"Dificuldade {dificuldade_codigo}.")
        else:
            frases.append(f"Dificuldade {dificuldade_codigo} ({dificuldade_nome.lower()}).")

    if not vazio(tempo_movimento_min):
        frases.append(f"Tempo em movimento: {formatar_duracao(tempo_movimento_min)}.")

    if not vazio(url):
        frases.append(f"Relato original: {url}")

    return " ".join(frases)


def montar_endereco(cidade=None, regiao=None, pais=None) -> str:
    """'Großarl, Salzburg, Áustria'; partes vazias ou repetidas são ignoradas."""
    partes = []
    for parte in (cidade, regiao, pais):
        if not vazio(parte) and parte not in partes:
            partes.append(parte)
    return ", ".join(partes) or "Localização não informada"
