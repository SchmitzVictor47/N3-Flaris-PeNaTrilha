"""Tabelas de referência e limites definidos na Avaliação N2."""

# Escala SAC (Swiss Alpine Club) com a tradução adotada na N2
ESCALA_SAC = {
    1: "Caminhada em vale",
    2: "Caminhada de montanha",
    3: "Caminhada de montanha difícil",
    4: "Caminhada alpina",
    5: "Caminhada alpina exigente",
    6: "Caminhada alpina difícil",
}

# Limites das regras de validação e limpeza (N2, item 1.e)
DISTANCIA_MAXIMA_M = 100_000
DISTANCIA_MINIMA_M = 500
VELOCIDADE_MAXIMA_KMH = 36
VELOCIDADE_MEDIA_ALERTA_KMH = 8
SUBIDA_ALERTA_M = 5_000
ANOS_INVALIDOS = (1, 1970)

# Motivos de quarentena, na ordem em que são avaliados
MOTIVOS_QUARENTENA = {
    "linha_malformada": "Linha do CSV que não pôde ser interpretada",
    "id_invalido": "_id ausente ou que não é um ObjectId",
    "gpx_xml_invalido": "GPX com XML inválido",
    "gpx_sem_trilha_gravada": "GPX sem trilha gravada (só rota planejada ou vazio)",
    "distancia_ausente": "Registro sem distância",
    "distancia_acima_100km": "Distância acima de 100 km (erro de GPS)",
    "distancia_abaixo_0_5km": "Distância abaixo de 0,5 km",
}

ALERTAS = {
    "provavel_bike_ou_esqui": "Velocidade média acima de 8 km/h",
    "subida_atipica": "Subida acima de 5.000 m",
}

# Contagens do perfilamento publicado na N2, usadas para comparar com cada lote
ESPERADO_N2 = {
    "linhas_lidas": 12_141,
    "duplicatas_removidas": 2_993,
    "trilhas_unicas": 9_148,
    "quarentena_total": 439,
    "quarentena": {
        "gpx_sem_trilha_gravada": 402,
        "distancia_acima_100km": 29,
        "distancia_abaixo_0_5km": 7,
        "gpx_xml_invalido": 1,
    },
    "trilhas_validas": 8_709,
    "sem_data_hora": 1_273,
    "tempo_invalido": 113,
    "sem_elevacao": 734,
    "velocidade_max_descartada": 99,
    "provavel_bike_ou_esqui": 311,
    "subida_atipica": 33,
    "nome_ajustado": 475,
    "trilhas_completas": 7_255,
    "trilheiros": 372,
    "percursos": 8_709,
}
