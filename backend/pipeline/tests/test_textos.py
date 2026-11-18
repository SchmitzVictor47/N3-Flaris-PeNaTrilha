from penatrilha_pipeline.textos import (
    formatar_duracao,
    formatar_numero,
    montar_descricao,
    montar_endereco,
)


def test_formata_numeros_no_padrao_brasileiro():
    assert formatar_numero(1323) == "1.323"
    assert formatar_numero(10.832953, 2) == "10,83"
    assert formatar_numero(613.0) == "613"


def test_formata_duracao():
    assert formatar_duracao(203) == "3 h 23 min"
    assert formatar_duracao(45) == "45 min"
    assert formatar_duracao(120) == "2 h"


def test_descricao_igual_ao_exemplo_da_n2():
    descricao = montar_descricao(
        distancia_km=10.83,
        subida_m=613,
        descida_m=610,
        altitude_min_m=1323,
        altitude_max_m=1934,
        dificuldade_codigo="T2",
        dificuldade_nome="Caminhada de montanha",
        tempo_movimento_min=203,
        url="http://www.hikr.org/tour/post131855.html",
    )
    assert descricao == (
        "Trilha de 10,83 km com 613 m de subida e 610 m de descida, entre 1.323 m e 1.934 m "
        "de altitude. Dificuldade T2 (caminhada de montanha). Tempo em movimento: 3 h 23 min. "
        "Relato original: http://www.hikr.org/tour/post131855.html"
    )


def test_descricao_omite_trechos_sem_dado():
    descricao = montar_descricao(
        distancia_km=5.5,
        subida_m=float("nan"),
        descida_m=None,
        dificuldade_codigo="T4+",
        dificuldade_nome="Caminhada alpina",
        tempo_movimento_min=None,
    )
    assert descricao == "Trilha de 5,50 km. Dificuldade T4+ (caminhada alpina)."


def test_endereco_ignora_partes_vazias_e_repetidas():
    assert montar_endereco("Großarl", "Salzburg", "Áustria") == "Großarl, Salzburg, Áustria"
    assert montar_endereco("Vaduz", "Vaduz", "Liechtenstein") == "Vaduz, Liechtenstein"
    assert montar_endereco(None, "", None) == "Localização não informada"
