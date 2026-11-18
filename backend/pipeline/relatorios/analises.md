# Análises do acervo importado (N2, item f)

Geradas em 17/11/2026 22:34 UTC, direto no MongoDB `penatrilha`, com pipelines de agregação e Python (pandas e scikit-learn).

## 1. Perfil de dificuldade

| Grau | Trilhas (todas) | Trilhas completas | Distância média | Subida média | Velocidade média |
| --- | --- | --- | --- | --- | --- |
| T1 | 815 | 582 | 12,2 km | 419 m | 4,56 km/h |
| T2 | 2.808 | 2.242 | 13,1 km | 867 m | 3,70 km/h |
| T3 | 2.994 | 2.595 | 14,8 km | 1.263 m | 3,47 km/h |
| T4 | 1.271 | 1.085 | 14,5 km | 1.318 m | 3,55 km/h |
| T5 | 606 | 554 | 13,5 km | 1.343 m | 3,47 km/h |
| T6 | 215 | 197 | 13,2 km | 1.314 m | 3,50 km/h |

**Registros incoerentes:** 7 trilhas T1 (caminhada em vale) têm mais de 2.000 m de subida — sinal de ruído na altimetria do GPS:

| Trilha | Distância | Subida | Endereço |
| --- | --- | --- | --- |
| Von Biel nach Sutz dem See entlang | 5,92 km | 8.663 m | Nidau, Bern, Suíça |
| Ein typischer Aargauer Cocktail: Flüsse, Biber, Industriekultur und Ruinen | 10,49 km | 6.982 m | Klingnau, Aargau, Suíça |
| Stürmische Winde an den Eggen | 23,21 km | 5.362 m | Sattel, Schwyz, Suíça |
| Jurapark II: Sonnenuntergang am Laubberg mit Paraglidern | 2,76 km | 3.875 m | Sulz, Aargau, Suíça |
| Valtellina - Via Terrazzamenti da Tirano a Morbegno | 82,69 km | 2.781 m | Tirano, Lombardy, Itália |
| Isola di Ponza trek | 48,16 km | 2.353 m | Ponza, Latium, Itália |
| Cascate di Lillaz e Valnontey | 18,36 km | 2.183 m | Cogne, Aosta Valley, Itália |

## 2. Sazonalidade (trilhas por mês de início)

| Mês | Trilhas |  |
| --- | --- | --- |
| janeiro | 521 | █████████████████ |
| fevereiro | 218 | ███████ |
| março | 322 | ██████████ |
| abril | 544 | ██████████████████ |
| maio | 633 | █████████████████████ |
| junho | 707 | ███████████████████████ |
| julho | 799 | ██████████████████████████ |
| agosto | 923 | ██████████████████████████████ |
| setembro | 769 | █████████████████████████ |
| outubro | 782 | █████████████████████████ |
| novembro | 527 | █████████████████ |
| dezembro | 578 | ███████████████████ |

## 3. Ranking de trilheiros

| # | Trilheiro | Trilhas | Distância total | Subida total | Dificuldade máxima |
| --- | --- | --- | --- | --- | --- |
| 1 | cristina | 406 | 7.939,87 km | 620.201 m | T5+ |
| 2 | ser59 | 329 | 5.832,88 km | 507.777 m | T6- |
| 3 | Felix | 312 | 3.225,69 km | 71.821 m | T5+ |
| 4 | Mo6451 | 275 | 3.059,91 km | 97.646 m | T6- |
| 5 | Poge | 239 | 4.634,94 km | 408.321 m | T5+ |
| 6 | Menek | 213 | 2.449,56 km | 195.581 m | T5+ |
| 7 | Riosambesi | 191 | 1.228,98 km | 155.602 m | T3+ |
| 8 | Gherard | 165 | 1.938,31 km | 94.858 m | T4 |
| 9 | Bergamotte | 142 | 2.110,06 km | 177.328 m | T6+ |
| 10 | SCM | 138 | 1.751,51 km | 82.786 m | T5 |

Perfil de exemplo (N2): **Bergfritz** soma 53 trilhas, 533,47 km e 35.865 m de subida; dificuldade máxima T4.

## 4. Busca por proximidade (“trilhas perto de mim”)

`$geoNear` a partir de Zermatt (Suíça), raio de 10 km: **24 trilhas** começam no raio. As cinco mais próximas:

| Trilha | Dificuldade | Distância da trilha | Distância do ponto |
| --- | --- | --- | --- |
| Neue Monte Rosa Hütte | T4 | 30,53 km | 0,17 km |
| Zermatter Breithorn (4164 m) mit Akklimatisation | T4 | 13,67 km | 0,28 km |
| Hohbalmen : en circuit depuis Zermatt, retour par Zmutt | T2 | 17,75 km | 0,28 km |
| Aufstieg zum Hotel du Trift | T3 | 3,71 km | 0,31 km |
| Einlauftürchen nach Zmutt | T2 | 3,66 km | 0,32 km |

## 5. Filtro por dificuldade e distância

Trilhas T3 entre 5 e 10 km: **647**. Plano de execução do MongoDB: `IXSCAN` no índice `dificuldade_grau_distancia_km`.

## 6. Concentração do acervo

**91,7%** das trilhas começam na região dos Alpes (7.987 de 8.709; retângulo 5,0°–16,5° L × 43,6°–48,4° N). Trilhas a até 50 km de Florianópolis, foco do app: **0** — é onde o acervo precisa crescer.

| País | Trilhas |
| --- | --- |
| Suíça | 3.507 |
| Itália | 3.062 |
| Alemanha | 734 |
| Áustria | 702 |
| França | 221 |
| Estados Unidos | 77 |
| Espanha | 72 |
| Liechtenstein | 33 |

| Região | País | Trilhas |
| --- | --- | --- |
| Lombardy | Itália | 1.599 |
| Piedmont | Itália | 702 |
| Ticino | Suíça | 693 |
| Bavaria | Alemanha | 519 |
| Grisons | Suíça | 466 |
| Valais | Suíça | 393 |
| Tyrol | Áustria | 374 |
| Bern | Suíça | 372 |

## 7. Estimativa de tempo de percurso

Regressão (gradient boosting) treinada com 5.804 das 7.255 trilhas completas e testada nas outras 1.451, com distância, subida, descida e grau SAC.

| Método | Erro médio absoluto no teste |
| --- | --- |
| Regra de Naismith (1 h / 5 km + 1 h / 600 m) | 50 min |
| Regressão treinada nos dados do hikr.org | 40 min |

## 8. Recomendação de trilhas parecidas

Para quem fez **Remsteinkopf, 1945 m** (10,83 km, 613 m de subida, T2), as trilhas mais parecidas em distância, subida e grau são:

| Trilha | Dificuldade | Distância | Subida | Endereço |
| --- | --- | --- | --- | --- |
| Monte Ferraro | T2 | 10,83 km | 619 m | Gravesano, Ticino, Suíça |
| Giro del pizzo Formico e rifugio Parafulmine | T2 | 10,73 km | 613 m | Gandino, Lombardy, Itália |
| EPIFANIA ai BALMIT (Monte Todano.) | T2 | 10,71 km | 611 m | Miazzina, Piedmont, Itália |
| Monte Bisbino (1325 m) via Piazzola | T2 | 10,69 km | 594 m | Maslianico, Lombardy, Itália |
| Bärenkopf | T2 | 10,61 km | 611 m | Stans, Tyrol, Áustria |
