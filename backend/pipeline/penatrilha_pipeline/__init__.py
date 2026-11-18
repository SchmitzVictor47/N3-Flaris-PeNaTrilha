"""Pipeline de Big Data do Pé na Trilha: hikr.org (Kaggle) → bronze → prata → ouro → MongoDB.

Implementação da Avaliação N3, seguindo a especificação da Avaliação N2.
Cada módulo corresponde a uma etapa do fluxo:

1. captura        – Kaggle API → camada bronze (.zip intacto + SHA-256)
2. leitura        – Spark lê o CSV multilinha com esquema explícito
3. validacao      – deduplicação, leitura do GPX e quarentena
4. limpeza        – padronização de textos, datas, unidades e dificuldade
5. transformacao  – pontos GPS, endereço, descrição e estatísticas (camada ouro)
6. armazenamento  – Parquet com Snappy nas camadas prata, ouro e quarentena
7. carga          – índices + upsert por _id no MongoDB (MongoDB Spark Connector)
8. verificacao    – confere as contagens e registra o lote
"""

__version__ = "1.0.0"
