"""Etapa 8: compara a camada ouro com o MongoDB, registra o lote e gera o relatório."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import pyarrow.dataset as ds
from bson import ObjectId, json_util

from . import armazenamento, config
from .mongo import conectar
from .referencias import ESPERADO_N2, MOTIVOS_QUARENTENA
from .transformacao import ID_EXEMPLO

log = logging.getLogger(__name__)
INDICES_OBRIGATORIOS = {
    "localizacao_inicio_2dsphere",
    "dificuldade_grau_distancia_km",
    "trilheiro_id",
    "titulo_texto",
}


def _contar_parquet(caminho) -> int:
    return ds.dataset(str(caminho), format="parquet").count_rows()


def _fmt(numero) -> str:
    if isinstance(numero, (int, float)):
        return f"{numero:,}".replace(",", ".")
    return str(numero)


def _comparar_com_n2(prata: dict, mongo: dict) -> list[dict]:
    obtido = {**prata, **{f"quarentena.{k}": v for k, v in prata.get("quarentena", {}).items()}}
    obtido["trilheiros"] = mongo["trilheiros"]
    obtido["percursos"] = mongo["percursos"]
    linhas = []
    for chave, esperado in ESPERADO_N2.items():
        if chave == "quarentena":
            for motivo, valor in esperado.items():
                n3 = obtido.get(f"quarentena.{motivo}", 0)
                linhas.append({"metrica": f"quarentena: {motivo}", "n2": valor, "n3": n3, "igual": n3 == valor})
            continue
        n3 = obtido.get(chave)
        linhas.append({"metrica": chave, "n2": esperado, "n3": n3, "igual": n3 == esperado})
    return linhas


def _markdown(lote: dict, exemplo_trilha, exemplo_trilheiro) -> str:
    p, m = lote["contagens"]["prata"], lote["contagens"]["mongodb"]
    c = lote["captura"]
    linhas = [
        f"# Relatório do lote {lote['_id']}",
        "",
        f"- **Status:** {lote['status']}",
        f"- **Origem da execução:** {lote['origem']}",
        f"- **Dataset:** {c['dataset']} (versão {c.get('versao_dataset')}, licença {c.get('licenca')})",
        f"- **Captura:** {c['metodo']} · {_fmt(c['tamanho_bytes'])} bytes · SHA-256 `{c['sha256']}`",
        f"- **Destino:** MongoDB `{config.MONGO_DB}`",
        f"- **Início / fim (UTC):** {lote['iniciado_em']:%d/%m/%Y %H:%M:%S} → "
        f"{lote['finalizado_em']:%d/%m/%Y %H:%M:%S} "
        f"({(lote['finalizado_em'] - lote['iniciado_em']).total_seconds():.0f} s)",
        "",
        "## Funil do lote",
        "",
        "| Etapa | Registros |",
        "| --- | ---: |",
        f"| Linhas lidas do CSV (bronze) | {_fmt(p['linhas_lidas'])} |",
        f"| Duplicatas removidas (mesma url) | {_fmt(p['duplicatas_removidas'])} |",
        f"| Trilhas únicas | {_fmt(p['trilhas_unicas'])} |",
        f"| Enviadas para a quarentena | {_fmt(p['quarentena_total'])} |",
        f"| Trilhas válidas (prata e ouro) | {_fmt(p['trilhas_validas'])} |",
        f"| Trilhas importadas no MongoDB | {_fmt(m['trilhas_importadas'])} |",
        f"| Trilheiros | {_fmt(m['trilheiros'])} |",
        f"| Percursos / pontos GPS | {_fmt(m['percursos'])} / {_fmt(m['pontos_gps'])} |",
        f"| Trilhas cadastradas no app (preservadas) | {_fmt(m['trilhas_manuais'])} |",
        "",
        "## Quarentena (Parquet, com o motivo)",
        "",
        "| Motivo | Registros |",
        "| --- | ---: |",
    ]
    for motivo, total in p["quarentena"].items():
        linhas.append(f"| {MOTIVOS_QUARENTENA.get(motivo, motivo)} | {_fmt(total)} |")
    linhas += [
        "",
        "## Correções e alertas (sobre as trilhas válidas)",
        "",
        "| Regra | Registros | Tratamento |",
        "| --- | ---: | --- |",
        f"| Sem data e hora | {_fmt(p['sem_data_hora'])} | tempos nulos, `qualidade.tem_tempo = false` |",
        f"| Ano 0001/1970, fim antes do início ou tempo em movimento 0 | {_fmt(p['tempo_invalido'])} | tempos nulos, `qualidade.tem_tempo = false` |",
        f"| Sem elevação | {_fmt(p['sem_elevacao'])} | altitudes, subida e descida nulas, `qualidade.tem_elevacao = false` |",
        f"| Velocidade máxima acima de 36 km/h | {_fmt(p['velocidade_max_descartada'])} | valor nulo |",
        f"| Velocidade média acima de 8 km/h | {_fmt(p['provavel_bike_ou_esqui'])} | alerta `provavel_bike_ou_esqui` |",
        f"| Subida acima de 5.000 m | {_fmt(p['subida_atipica'])} | alerta `subida_atipica` |",
        f"| Nome com espaços extras | {_fmt(p['nome_ajustado'])} | espaços aparados e unificados |",
        f"| Trilhas com tempo e elevação completos | {_fmt(p['trilhas_completas'])} | base das análises de duração |",
        "",
        "## Comparação com o perfilamento da N2",
        "",
        "| Métrica | N2 (especificação) | N3 (este lote) | Confere |",
        "| --- | ---: | ---: | :---: |",
    ]
    for item in lote["comparacao_n2"]:
        linhas.append(
            f"| {item['metrica']} | {_fmt(item['n2'])} | {_fmt(item['n3'])} | {'✅' if item['igual'] else '⚠️'} |"
        )
    linhas += [
        "",
        "## Verificação ouro × MongoDB",
        "",
        f"- Camada ouro: {_fmt(lote['contagens']['ouro']['trilhas'])} trilhas, "
        f"{_fmt(lote['contagens']['ouro']['trilheiros'])} trilheiros, "
        f"{_fmt(lote['contagens']['ouro']['percursos'])} percursos",
        f"- MongoDB: {_fmt(m['trilhas_importadas'])} trilhas importadas, {_fmt(m['trilheiros'])} trilheiros, "
        f"{_fmt(m['percursos'])} percursos",
        f"- `_id` que não são ObjectId: {m['ids_nao_objectid']} · títulos que não são texto: {m['titulos_nao_texto']}",
        f"- Índices de `trilhas`: {', '.join(sorted(m['indices']))}",
        f"- Divergências: {', '.join(lote['divergencias']) if lote['divergencias'] else 'nenhuma'}",
        "",
        "## Documento de exemplo em `trilhas` (primeiro registro do dataset)",
        "",
        "```json",
        json_util.dumps(exemplo_trilha, indent=2, ensure_ascii=False, json_options=json_util.RELAXED_JSON_OPTIONS),
        "```",
        "",
        "> O exemplo da N2 trazia o endereço “Großarl, Salzburg, Áustria”, obtido no Nominatim. "
        "O pipeline usa o reverse_geocoder (GeoNames, offline), que devolve a cidade mais próxima "
        "do ponto inicial.",
        "",
        "## Documento de exemplo em `trilheiros`",
        "",
        "```json",
        json_util.dumps(exemplo_trilheiro, indent=2, ensure_ascii=False, json_options=json_util.RELAXED_JSON_OPTIONS),
        "```",
        "",
    ]
    return "\n".join(linhas)


def verificar(lote_id: str, captura: dict, origem: str = "cli") -> dict:
    prata = armazenamento.ler_relatorio_etapa(lote_id, "prata")
    carga = armazenamento.ler_relatorio_etapa(lote_id, "carga")
    preparo = armazenamento.ler_relatorio_etapa(lote_id, "preparar_mongodb")
    ouro = {
        "trilhas": _contar_parquet(config.OURO_TRILHAS),
        "trilheiros": _contar_parquet(config.OURO_TRILHEIROS),
        "percursos": _contar_parquet(config.OURO_PERCURSOS),
    }

    cliente, banco = conectar()
    try:
        importadas = {"origem.fonte": config.FONTE}
        pontos = list(
            banco.percursos.aggregate([{"$group": {"_id": None, "total": {"$sum": "$total_pontos"}}}])
        )
        mongo = {
            "trilhas_total": banco.trilhas.count_documents({}),
            "trilhas_importadas": banco.trilhas.count_documents(importadas),
            "trilhas_manuais": banco.trilhas.count_documents({"origem.fonte": "manual"}),
            "trilhas_completas": banco.trilhas.count_documents(
                {**importadas, "qualidade.tem_tempo": True, "qualidade.tem_elevacao": True}
            ),
            "trilheiros": banco.trilheiros.count_documents({"origem": config.FONTE}),
            "percursos": banco.percursos.count_documents({}),
            "pontos_gps": pontos[0]["total"] if pontos else 0,
            "ids_nao_objectid": banco.trilhas.count_documents(
                {**importadas, "_id": {"$not": {"$type": "objectId"}}}
            ),
            "titulos_nao_texto": banco.trilhas.count_documents(
                {**importadas, "titulo": {"$not": {"$type": "string"}}}
            ),
            "indices": list(banco.trilhas.index_information().keys()),
        }
        exemplo_trilha = banco.trilhas.find_one({"_id": ObjectId(ID_EXEMPLO)})
        exemplo_trilheiro = banco.trilheiros.find_one({"_id": "hikr:Bergfritz"})

        enviados = carga.get("documentos_enviados", {})
        divergencias = []
        if mongo["trilhas_importadas"] != enviados.get("trilhas"):
            divergencias.append(f"trilhas: ouro {enviados.get('trilhas')} × MongoDB {mongo['trilhas_importadas']}")
        if mongo["percursos"] != enviados.get("percursos"):
            divergencias.append(f"percursos: ouro {enviados.get('percursos')} × MongoDB {mongo['percursos']}")
        if mongo["trilheiros"] != enviados.get("trilheiros"):
            divergencias.append(f"trilheiros: ouro {enviados.get('trilheiros')} × MongoDB {mongo['trilheiros']}")
        if mongo["ids_nao_objectid"] or mongo["titulos_nao_texto"]:
            divergencias.append("tipos inválidos em _id ou titulo")
        faltando = INDICES_OBRIGATORIOS - set(mongo["indices"])
        if faltando:
            divergencias.append(f"índices ausentes: {', '.join(sorted(faltando))}")
        if exemplo_trilha is None:
            divergencias.append(f"trilha de exemplo {ID_EXEMPLO} não encontrada por ObjectId")

        lote = {
            "_id": lote_id,
            "status": "falha" if divergencias else "sucesso",
            "origem": origem,
            "dataset": config.DATASET_KAGGLE,
            "sha256": captura["sha256"],
            "captura": captura,
            "iniciado_em": datetime.fromisoformat(captura["capturado_em"]),
            "finalizado_em": datetime.now(timezone.utc),
            "contagens": {"prata": prata, "ouro": ouro, "carga": carga, "mongodb": mongo},
            "preparo_mongodb": preparo,
            "comparacao_n2": _comparar_com_n2(prata, mongo),
            "divergencias": divergencias,
        }
        banco.lotes.replace_one({"_id": lote_id}, lote, upsert=True)
    finally:
        cliente.close()

    config.DIR_RELATORIOS.mkdir(parents=True, exist_ok=True)
    texto = _markdown(lote, exemplo_trilha, exemplo_trilheiro)
    (config.DIR_RELATORIOS / f"lote_{lote_id}.md").write_text(texto, encoding="utf-8")
    (config.DIR_RELATORIOS / "ultimo_lote.md").write_text(texto, encoding="utf-8")
    armazenamento.salvar_json(config.DIR_RELATORIOS / f"lote_{lote_id}.json", lote)

    if divergencias:
        raise RuntimeError(f"Verificação do lote {lote_id} falhou: {divergencias}")
    log.info(
        "Lote %s verificado: %s trilhas, %s trilheiros, %s percursos no MongoDB",
        lote_id, mongo["trilhas_importadas"], mongo["trilheiros"], mongo["percursos"],
    )
    return {"lote_id": lote_id, "status": lote["status"], "mongodb": mongo}
