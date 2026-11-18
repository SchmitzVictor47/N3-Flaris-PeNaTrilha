"""Análises do item f da N2, executadas no MongoDB depois da carga.

Gera relatorios/analises.md (para leitura) e relatorios/analises.json.
Uso fora do Airflow: python -m penatrilha_pipeline.analises
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler

from . import armazenamento, config
from .mongo import conectar
from .textos import formatar_numero
from .transformacao import ID_EXEMPLO

log = logging.getLogger(__name__)

MESES = [
    "janeiro", "fevereiro", "março", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
]
RAIO_TERRA_KM = 6378.1
ZERMATT = [7.7491, 46.0207]  # [longitude, latitude]
FLORIANOPOLIS = [-48.5480, -27.5954]
ALPES = {  # retângulo aproximado da região dos Alpes
    "type": "Polygon",
    "coordinates": [[[5.0, 43.6], [16.5, 43.6], [16.5, 48.4], [5.0, 48.4], [5.0, 43.6]]],
}


def _n(valor, casas=0) -> str:
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return "—"
    return formatar_numero(valor, casas)


def _tabela(cabecalho: list[str], linhas: list[list]) -> list[str]:
    saida = ["| " + " | ".join(cabecalho) + " |", "| " + " | ".join("---" for _ in cabecalho) + " |"]
    saida += ["| " + " | ".join(str(c) for c in linha) + " |" for linha in linhas]
    return saida + [""]


def _naismith_min(distancia_km, subida_m):
    """Regra de Naismith: 1 h a cada 5 km, mais 1 h a cada 600 m de subida."""
    return 60 * (distancia_km / 5) + 60 * (subida_m / 600)


def gerar() -> dict:
    cliente, banco = conectar()
    importadas = {"origem.fonte": config.FONTE}
    completas = {**importadas, "qualidade.tem_tempo": True, "qualidade.tem_elevacao": True}
    md = [
        "# Análises do acervo importado (N2, item f)",
        "",
        f"Geradas em {datetime.now(timezone.utc):%d/%m/%Y %H:%M} UTC, direto no MongoDB `{config.MONGO_DB}`, "
        "com pipelines de agregação e Python (pandas e scikit-learn).",
        "",
    ]
    resultado: dict = {}
    try:
        # 1. Perfil de dificuldade (mesma consulta da N2)
        perfil = list(
            banco.trilhas.aggregate(
                [
                    {"$match": {"qualidade.tem_tempo": True, "qualidade.tem_elevacao": True}},
                    {
                        "$group": {
                            "_id": "$dificuldade.grau",
                            "trilhas": {"$sum": 1},
                            "distancia_media_km": {"$avg": "$metricas.distancia_km"},
                            "subida_media_m": {"$avg": "$metricas.subida_m"},
                            "velocidade_media_kmh": {"$avg": "$metricas.velocidade_media_kmh"},
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            )
        )
        por_grau = {
            d["_id"]: d["total"]
            for d in banco.trilhas.aggregate(
                [{"$match": importadas}, {"$group": {"_id": "$dificuldade.grau", "total": {"$sum": 1}}}]
            )
        }
        resultado["perfil_dificuldade"] = perfil
        resultado["trilhas_por_grau"] = por_grau
        md += ["## 1. Perfil de dificuldade", ""]
        md += _tabela(
            ["Grau", "Trilhas (todas)", "Trilhas completas", "Distância média", "Subida média", "Velocidade média"],
            [
                [
                    f"T{d['_id']}",
                    _n(por_grau.get(d["_id"])),
                    _n(d["trilhas"]),
                    f"{_n(d['distancia_media_km'], 1)} km",
                    f"{_n(d['subida_media_m'])} m",
                    f"{_n(d['velocidade_media_kmh'], 2)} km/h",
                ]
                for d in perfil
            ],
        )

        incoerentes = list(
            banco.trilhas.find(
                {**importadas, "dificuldade.grau": 1, "metricas.subida_m": {"$gt": 2000}},
                {"titulo": 1, "endereco": 1, "metricas.distancia_km": 1, "metricas.subida_m": 1},
            ).sort("metricas.subida_m", -1)
        )
        resultado["t1_com_subida_acima_2000m"] = len(incoerentes)
        md += [
            f"**Registros incoerentes:** {len(incoerentes)} trilhas T1 (caminhada em vale) têm mais de "
            "2.000 m de subida — sinal de ruído na altimetria do GPS:",
            "",
        ]
        md += _tabela(
            ["Trilha", "Distância", "Subida", "Endereço"],
            [
                [
                    d["titulo"],
                    f"{_n(d['metricas']['distancia_km'], 2)} km",
                    f"{_n(d['metricas']['subida_m'])} m",
                    d["endereco"],
                ]
                for d in incoerentes
            ],
        )

        # 2. Sazonalidade
        sazonal = list(
            banco.trilhas.aggregate(
                [
                    {"$match": {**importadas, "periodo.inicio": {"$ne": None}}},
                    {"$group": {"_id": {"$month": "$periodo.inicio"}, "trilhas": {"$sum": 1}}},
                    {"$sort": {"_id": 1}},
                ]
            )
        )
        resultado["sazonalidade"] = {MESES[d["_id"] - 1]: d["trilhas"] for d in sazonal}
        maximo = max((d["trilhas"] for d in sazonal), default=1)
        md += ["## 2. Sazonalidade (trilhas por mês de início)", ""]
        md += _tabela(
            ["Mês", "Trilhas", ""],
            [[MESES[d["_id"] - 1], _n(d["trilhas"]), "█" * round(30 * d["trilhas"] / maximo)] for d in sazonal],
        )

        # 3. Ranking de trilheiros (lido direto da coleção trilheiros)
        ranking = list(banco.trilheiros.find().sort("estatisticas.total_trilhas", -1).limit(10))
        resultado["ranking_trilheiros"] = [
            {"usuario": d["usuario"], **d["estatisticas"]} for d in ranking
        ]
        md += ["## 3. Ranking de trilheiros", ""]
        md += _tabela(
            ["#", "Trilheiro", "Trilhas", "Distância total", "Subida total", "Dificuldade máxima"],
            [
                [
                    i,
                    d["usuario"],
                    _n(d["estatisticas"]["total_trilhas"]),
                    f"{_n(d['estatisticas']['distancia_total_km'], 2)} km",
                    f"{_n(d['estatisticas']['subida_total_m'])} m",
                    d["estatisticas"]["dificuldade_maxima"],
                ]
                for i, d in enumerate(ranking, start=1)
            ],
        )
        bergfritz = banco.trilheiros.find_one({"_id": "hikr:Bergfritz"})
        if bergfritz:
            e = bergfritz["estatisticas"]
            md += [
                f"Perfil de exemplo (N2): **Bergfritz** soma {_n(e['total_trilhas'])} trilhas, "
                f"{_n(e['distancia_total_km'], 2)} km e {_n(e['subida_total_m'])} m de subida; "
                f"dificuldade máxima {e['dificuldade_maxima']}.",
                "",
            ]

        # 4. Busca por proximidade com $geoNear (índice 2dsphere)
        proximas = list(
            banco.trilhas.aggregate(
                [
                    {
                        "$geoNear": {
                            "near": {"type": "Point", "coordinates": ZERMATT},
                            "distanceField": "distancia_ate_ponto_m",
                            "maxDistance": 10_000,
                            "spherical": True,
                            "query": importadas,
                        }
                    },
                    {"$limit": 5},
                    {
                        "$project": {
                            "titulo": 1,
                            "dificuldade.codigo": 1,
                            "metricas.distancia_km": 1,
                            "distancia_ate_ponto_m": 1,
                        }
                    },
                ]
            )
        )
        no_raio = banco.trilhas.count_documents(
            {"localizacao.inicio": {"$geoWithin": {"$centerSphere": [ZERMATT, 10 / RAIO_TERRA_KM]}}}
        )
        perto_floripa = banco.trilhas.count_documents(
            {"localizacao.inicio": {"$geoWithin": {"$centerSphere": [FLORIANOPOLIS, 50 / RAIO_TERRA_KM]}}}
        )
        resultado["proximidade_zermatt_10km"] = {"total": no_raio, "mais_proximas": proximas}
        resultado["trilhas_ate_50km_de_florianopolis"] = perto_floripa
        md += [
            "## 4. Busca por proximidade (“trilhas perto de mim”)",
            "",
            f"`$geoNear` a partir de Zermatt (Suíça), raio de 10 km: **{_n(no_raio)} trilhas** começam no raio. "
            "As cinco mais próximas:",
            "",
        ]
        md += _tabela(
            ["Trilha", "Dificuldade", "Distância da trilha", "Distância do ponto"],
            [
                [
                    d["titulo"],
                    d.get("dificuldade", {}).get("codigo", "—"),
                    f"{_n(d['metricas']['distancia_km'], 2)} km",
                    f"{_n(d['distancia_ate_ponto_m'] / 1000, 2)} km",
                ]
                for d in proximas
            ],
        )

        # 5. Filtro por dificuldade e distância (índice composto)
        filtro = {"dificuldade.grau": 3, "metricas.distancia_km": {"$gte": 5, "$lte": 10}}
        total_filtro = banco.trilhas.count_documents(filtro)
        plano = banco.trilhas.find(filtro).explain()["queryPlanner"]["winningPlan"]
        indice_usado = plano.get("inputStage", {}).get("indexName") or plano.get("indexName")
        resultado["filtro_t3_5a10km"] = {"total": total_filtro, "indice": indice_usado}
        md += [
            "## 5. Filtro por dificuldade e distância",
            "",
            f"Trilhas T3 entre 5 e 10 km: **{_n(total_filtro)}**. "
            f"Plano de execução do MongoDB: `IXSCAN` no índice `{indice_usado}`.",
            "",
        ]

        # 6. Concentração do acervo (base de um mapa de calor)
        por_pais = list(
            banco.trilhas.aggregate(
                [
                    {"$match": importadas},
                    {"$group": {"_id": "$localizacao.pais", "trilhas": {"$sum": 1}}},
                    {"$sort": {"trilhas": -1}},
                    {"$limit": 8},
                ]
            )
        )
        por_regiao = list(
            banco.trilhas.aggregate(
                [
                    {"$match": importadas},
                    {
                        "$group": {
                            "_id": {"regiao": "$localizacao.regiao", "pais": "$localizacao.pais"},
                            "trilhas": {"$sum": 1},
                        }
                    },
                    {"$sort": {"trilhas": -1}},
                    {"$limit": 8},
                ]
            )
        )
        total_importadas = banco.trilhas.count_documents(importadas)
        nos_alpes = banco.trilhas.count_documents(
            {**importadas, "localizacao.inicio": {"$geoWithin": {"$geometry": ALPES}}}
        )
        resultado["concentracao"] = {
            "por_pais": {d["_id"]: d["trilhas"] for d in por_pais},
            "nos_alpes": nos_alpes,
            "percentual_alpes": round(100 * nos_alpes / max(total_importadas, 1), 1),
        }
        md += [
            "## 6. Concentração do acervo",
            "",
            f"**{_n(100 * nos_alpes / max(total_importadas, 1), 1)}%** das trilhas começam na região dos Alpes "
            f"({_n(nos_alpes)} de {_n(total_importadas)}; retângulo 5,0°–16,5° L × 43,6°–48,4° N). "
            f"Trilhas a até 50 km de Florianópolis, foco do app: **{_n(perto_floripa)}** — é onde o acervo precisa crescer.",
            "",
        ]
        md += _tabela(["País", "Trilhas"], [[d["_id"], _n(d["trilhas"])] for d in por_pais])
        md += _tabela(
            ["Região", "País", "Trilhas"],
            [[d["_id"].get("regiao"), d["_id"].get("pais"), _n(d["trilhas"])] for d in por_regiao],
        )

        # 7. Estimativa de tempo de percurso: regressão × regra de Naismith
        documentos = list(
            banco.trilhas.find(
                completas,
                {
                    "dificuldade.grau": 1,
                    "metricas.distancia_km": 1,
                    "metricas.subida_m": 1,
                    "metricas.descida_m": 1,
                    "metricas.tempo_movimento_min": 1,
                },
            )
        )
        tabela = pd.DataFrame(
            [
                {
                    "_id": str(d["_id"]),
                    "grau": (d.get("dificuldade") or {}).get("grau"),
                    **{k: d["metricas"].get(k) for k in ("distancia_km", "subida_m", "descida_m", "tempo_movimento_min")},
                }
                for d in documentos
            ]
        ).dropna()
        atributos = ["distancia_km", "subida_m", "descida_m", "grau"]
        treino, teste = train_test_split(tabela, test_size=0.2, random_state=42)
        modelo = GradientBoostingRegressor(random_state=42).fit(treino[atributos], treino["tempo_movimento_min"])
        previsto = modelo.predict(teste[atributos])
        naismith = _naismith_min(teste["distancia_km"], teste["subida_m"])
        erro_modelo = mean_absolute_error(teste["tempo_movimento_min"], previsto)
        erro_naismith = mean_absolute_error(teste["tempo_movimento_min"], naismith)
        resultado["estimativa_tempo"] = {
            "trilhas_completas": len(tabela),
            "treino": len(treino),
            "teste": len(teste),
            "erro_medio_regressao_min": round(float(erro_modelo), 1),
            "erro_medio_naismith_min": round(float(erro_naismith), 1),
        }
        md += [
            "## 7. Estimativa de tempo de percurso",
            "",
            f"Regressão (gradient boosting) treinada com {_n(len(treino))} das {_n(len(tabela))} trilhas completas "
            f"e testada nas outras {_n(len(teste))}, com distância, subida, descida e grau SAC.",
            "",
        ]
        md += _tabela(
            ["Método", "Erro médio absoluto no teste"],
            [
                ["Regra de Naismith (1 h / 5 km + 1 h / 600 m)", f"{_n(erro_naismith)} min"],
                ["Regressão treinada nos dados do hikr.org", f"{_n(erro_modelo)} min"],
            ],
        )

        # 8. Recomendação: vizinhos mais próximos em distância, subida e grau
        referencia = tabela[tabela["_id"] == ID_EXEMPLO]
        if not referencia.empty:
            escala = StandardScaler().fit(tabela[["distancia_km", "subida_m", "grau"]])
            vizinhos = NearestNeighbors(n_neighbors=6).fit(
                escala.transform(tabela[["distancia_km", "subida_m", "grau"]])
            )
            _, indices = vizinhos.kneighbors(escala.transform(referencia[["distancia_km", "subida_m", "grau"]]))
            ids = [tabela.iloc[i]["_id"] for i in indices[0] if tabela.iloc[i]["_id"] != ID_EXEMPLO][:5]
            from bson import ObjectId

            parecidas = {
                str(d["_id"]): d
                for d in banco.trilhas.find(
                    {"_id": {"$in": [ObjectId(i) for i in ids]}},
                    {"titulo": 1, "endereco": 1, "dificuldade.codigo": 1, "metricas.distancia_km": 1, "metricas.subida_m": 1},
                )
            }
            resultado["recomendacao"] = {"referencia": ID_EXEMPLO, "parecidas": ids}
            md += [
                "## 8. Recomendação de trilhas parecidas",
                "",
                "Para quem fez **Remsteinkopf, 1945 m** (10,83 km, 613 m de subida, T2), "
                "as trilhas mais parecidas em distância, subida e grau são:",
                "",
            ]
            md += _tabela(
                ["Trilha", "Dificuldade", "Distância", "Subida", "Endereço"],
                [
                    [
                        parecidas[i]["titulo"],
                        parecidas[i]["dificuldade"]["codigo"],
                        f"{_n(parecidas[i]['metricas']['distancia_km'], 2)} km",
                        f"{_n(parecidas[i]['metricas']['subida_m'])} m",
                        parecidas[i]["endereco"],
                    ]
                    for i in ids
                    if i in parecidas
                ],
            )
    finally:
        cliente.close()

    config.DIR_RELATORIOS.mkdir(parents=True, exist_ok=True)
    (config.DIR_RELATORIOS / "analises.md").write_text("\n".join(md), encoding="utf-8")
    armazenamento.salvar_json(config.DIR_RELATORIOS / "analises.json", resultado)
    log.info("Análises gravadas em %s", config.DIR_RELATORIOS / "analises.md")
    return {
        "perfil_dificuldade": len(resultado.get("perfil_dificuldade", [])),
        "estimativa_tempo": resultado.get("estimativa_tempo"),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    gerar()
    print((config.DIR_RELATORIOS / "analises.md").read_text(encoding="utf-8"))
