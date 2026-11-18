# N3 — Pé na Trilha: pipeline de Big Data para MongoDB

**Avaliação N3 — Desenvolvimento com Linguagens Disruptivas I**
Equipe: Davi Martinelli Landal, Victor Schmitz Alvim de Oliveira e Hiago Farias Veloso.

Implementação do pipeline de ingestão especificado na Avaliação N2. As 12.141 linhas do dataset
[GPS recorded hikes from hikr.org](https://www.kaggle.com/datasets/roccoli/gpx-hike-tracks) (Kaggle) são
capturadas, limpas, enriquecidas e carregadas no MongoDB do app Pé na Trilha: **8.709 trilhas, 372 trilheiros e
8.709 percursos com 1,45 milhão de pontos GPS**. Todas as contagens batem com o perfilamento da N2.

**Stack:** Apache Airflow · Apache Spark (PySpark) · pandas · gpxpy · reverse_geocoder · Parquet · MongoDB Spark
Connector · MongoDB · Docker Compose · Node.js/Express/Mongoose · React.

## Estrutura

```text
N3-Flaris-PeNaTrilha/
├── backend/                 # API Node/Express do Pé na Trilha
│   └── pipeline/            # ← pipeline de Big Data da N3 (Airflow + Spark + MongoDB)
│       ├── dags/            # DAG do Airflow (8 tarefas)
│       ├── penatrilha_pipeline/  # uma etapa da N2 por módulo
│       ├── relatorios/      # saídas da execução (funil, comparação N2 × N3, análises)
│       └── README.md        # instruções completas de execução
└── frontend/                # app React do Pé na Trilha
```

## Como executar

**Instruções completas: [backend/pipeline/README.md](backend/pipeline/README.md).** Resumo:

1. Com o Docker Desktop aberto (6 GB de memória ou mais), suba o pipeline:

   ```bash
   cd backend/pipeline
   docker compose up -d --build
   ```

2. Abra o Airflow em <http://localhost:8080> (usuário `admin`, senha `admin`), clique na DAG
   `pe_na_trilha_ingestao` e em **Trigger DAG**. As 8 tarefas terminam em cerca de 5 minutos.
3. Veja as saídas em [backend/pipeline/relatorios/](backend/pipeline/relatorios/) e o banco em
   <http://localhost:8081> (`admin` / `admin`, banco `penatrilha`).
4. Opcional, a API e o app com as trilhas importadas:

   ```bash
   # backend: crie o .env a partir do .env.example com MONGO_URI=mongodb://localhost:27017/penatrilha
   cd backend && npm install && npm run dev
   # frontend (outro terminal)
   cd frontend/pe-na-trilha && npm install && npm run dev
   ```

   Depois abra <http://localhost:5173/trilhas>.

## Saídas esperadas

| Etapa | Registros |
| --- | ---: |
| Linhas lidas do CSV | 12.141 |
| Duplicatas removidas (mesma url) | 2.993 |
| Enviadas para a quarentena (GPX sem trilha, distância inválida, XML inválido) | 439 |
| **Trilhas carregadas no MongoDB** | **8.709** |
| Trilheiros / pontos GPS | 372 / 1.452.312 |

Relatório completo da execução: [backend/pipeline/relatorios/ultimo_lote.md](backend/pipeline/relatorios/ultimo_lote.md) ·
análises: [backend/pipeline/relatorios/analises.md](backend/pipeline/relatorios/analises.md).
