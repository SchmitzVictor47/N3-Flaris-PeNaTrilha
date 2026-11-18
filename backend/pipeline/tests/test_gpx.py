from penatrilha_pipeline.gpx import analisar_gpx, extrair_pontos

GPX_GRAVADO = """<?xml version="1.0" encoding="UTF-8"?>
<gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1" creator="Wanderkarte 4.0.2">
  <trk><name>remsteinkopf</name><trkseg>
    <trkpt lat="47.231143" lon="13.227007"><ele>1322.96</ele><time>2018-05-11T07:37:40Z</time></trkpt>
    <trkpt lat="47.230543" lon="13.227488"><ele>1330.89</ele><time>1970-01-01T00:00:00Z</time></trkpt>
  </trkseg></trk>
</gpx>"""

GPX_SO_ROTA = """<?xml version="1.0"?>
<gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1" creator="planejador">
  <rte><rtept lat="46.0" lon="7.7"/><rtept lat="46.1" lon="7.8"/></rte>
</gpx>"""

GPX_SEM_ELEVACAO = """<?xml version="1.0"?>
<gpx xmlns="http://www.topografix.com/GPX/1/0" version="1.0" creator="app">
  <trk><trkseg><trkpt lat="46.0" lon="7.7"/><trkpt lat="46.1" lon="7.8"/></trkseg></trk>
</gpx>"""


def test_gpx_com_trilha_gravada():
    status = analisar_gpx(GPX_GRAVADO)
    assert status["gpx_status"] == "ok"
    assert status["gpx_versao"] == "1.1"
    assert status["gpx_criador"] == "Wanderkarte 4.0.2"
    assert status["gpx_total_pontos"] == 2
    assert status["gpx_pontos_com_elevacao"] == 2


def test_gpx_so_com_rota_planejada_nao_tem_trilha_gravada():
    assert analisar_gpx(GPX_SO_ROTA)["gpx_status"] == "sem_trilha"


def test_gpx_vazio_ou_invalido():
    assert analisar_gpx("")["gpx_status"] == "sem_trilha"
    assert analisar_gpx("<gpx><trk>")["gpx_status"] == "xml_invalido"


def test_gpx_sem_tag_ele():
    status = analisar_gpx(GPX_SEM_ELEVACAO)
    assert status["gpx_status"] == "ok"
    assert status["gpx_pontos_com_elevacao"] == 0


def test_extrai_pontos_e_ponto_inicial():
    dados = extrair_pontos(GPX_GRAVADO)
    assert dados["total_pontos"] == 2
    assert (dados["inicio_lat"], dados["inicio_lon"]) == (47.231143, 13.227007)
    primeiro, segundo = dados["pontos"]
    assert primeiro == {"t_ms": 1526024260000, "lat": 47.231143, "lon": 13.227007, "ele": 1322.96}
    assert segundo["t_ms"] is None  # ano 1970 vira nulo
