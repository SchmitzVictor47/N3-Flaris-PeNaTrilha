# Pé na Trilha — Backend

API REST para cadastrar e gerenciar trilhas. O projeto oferece operações de criação, consulta, atualização e exclusão, com validação dos dados, persistência no MongoDB e tratamento centralizado de erros.

> **Avaliação N3 — pipeline de Big Data.** A pasta [`pipeline/`](pipeline/README.md) contém o fluxo de ingestão
> especificado na N2: Airflow + Spark + Parquet carregam no MongoDB 8.709 trilhas reais gravadas por GPS
> (dataset do hikr.org no Kaggle), que esta API passa a servir. As instruções de execução e as saídas esperadas
> estão em [pipeline/README.md](pipeline/README.md).

## Tecnologias

- Node.js e ES Modules
- Express 5
- MongoDB e Mongoose
- CORS e dotenv
- Jest para testes
- ESLint para análise de código

## Pré-requisitos

Antes de começar, tenha instalado:

- [Node.js](https://nodejs.org/) 20 ou superior
- npm
- Uma instância do [MongoDB](https://www.mongodb.com/), local ou no MongoDB Atlas

## Instalação

1. Clone o repositório e acesse a pasta do projeto:

   ```bash
   git clone <URL_DO_REPOSITORIO>
   cd pe-na-trilha-backend
   ```

2. Instale as dependências:

   ```bash
   npm install
   ```

3. Crie o arquivo de variáveis de ambiente a partir do exemplo:

   ```bash
   cp .env.example .env
   ```

4. Preencha o `.env` com os dados do seu ambiente:

   ```env
   PORT=3000
   MONGO_URI=mongodb+srv://usuario:senha@cluster.mongodb.net/penatrilha
   FRONTEND_URL=http://localhost:5173
   ```

   | Variável | Obrigatória | Descrição | Padrão |
   | --- | --- | --- | --- |
   | `MONGO_URI` | Sim | URI de conexão com o MongoDB | — |
   | `PORT` | Não | Porta utilizada pela API | `3000` |
   | `FRONTEND_URL` | Não | Origem autorizada pelo CORS | `http://localhost:5173` |

> Não publique o arquivo `.env`: ele pode conter credenciais e já está incluído no `.gitignore`.

## Executando o projeto

Em desenvolvimento, com reinicialização automática:

```bash
npm run dev
```

Em modo normal:

```bash
npm start
```

Por padrão, a API estará disponível em `http://localhost:3000`.

## Endpoints

A URL base dos recursos é `/trilhas`.

| Método | Endpoint | Descrição | Sucesso |
| --- | --- | --- | --- |
| `GET` | `/trilhas` | Lista as trilhas, das mais recentes para as mais antigas, com filtros e paginação | `200` |
| `GET` | `/trilhas/proximas` | Lista as trilhas que começam perto de um ponto (`lat`, `lon`, `raioKm`, `limite`) | `200` |
| `GET` | `/trilhas/:id` | Busca uma trilha pelo ID | `200` |
| `GET` | `/trilhas/:id/percurso` | Pontos GPS gravados de uma trilha importada | `200` |
| `POST` | `/trilhas` | Cadastra uma trilha | `201` |
| `PUT` | `/trilhas/:id` | Atualiza todos os dados de uma trilha | `200` |
| `DELETE` | `/trilhas/:id` | Exclui uma trilha | `204` |
| `GET` | `/trilheiros` | Ranking de trilheiros por número de trilhas (`limite`) | `200` |
| `GET` | `/trilheiros/:id` | Perfil de um trilheiro (ex.: `hikr:Bergfritz`) | `200` |
| `GET` | `/analises/dificuldade` | Distância, subida e velocidade médias por grau SAC | `200` |
| `GET` | `/analises/sazonalidade` | Trilhas por mês de início | `200` |

### Filtros de `GET /trilhas`

Todos são opcionais. O total de resultados vem no cabeçalho `X-Total-Count`.

| Parâmetro | Exemplo | Efeito |
| --- | --- | --- |
| `limite` | `20` | Trilhas por página, de 1 a 100 (padrão `50`) |
| `pagina` | `2` | Página, a partir de `1` |
| `dificuldade` | `3` | Grau SAC de 1 a 6 |
| `distanciaMin` / `distanciaMax` | `5` / `10` | Distância em km |
| `busca` | `piz` | Trecho do título, sem diferenciar maiúsculas |
| `fonte` | `manual` ou `hikr.org` | Origem da trilha |
| `pais` | `Suíça` | País do ponto de partida |

```bash
curl "http://localhost:3000/trilhas?dificuldade=3&distanciaMin=5&distanciaMax=10&limite=5"
curl "http://localhost:3000/trilhas/proximas?lat=46.0207&lon=7.7491&raioKm=10"
```

### Formato da trilha

Os campos `titulo`, `descricao` e `endereco` são obrigatórios e devem ser textos não vazios.

```json
{
  "titulo": "Trilha da Lagoa",
  "descricao": "Percurso com vista para a lagoa e mata nativa.",
  "endereco": "Florianópolis, SC"
}
```

As trilhas importadas pelo pipeline trazem também `dificuldade`, `localizacao` (ponto GeoJSON, cidade, região e país),
`metricas`, `periodo`, `trilheiro`, `qualidade` e `origem`. Esses campos são opcionais no cadastro manual, que
recebe `origem.fonte = "manual"` e nunca é sobrescrito pela carga.

Ao salvar uma trilha, a API acrescenta o `_id`, `createdAt` e `updatedAt`:

```json
{
  "_id": "685865727ad75b64e3d85abc",
  "titulo": "Trilha da Lagoa",
  "descricao": "Percurso com vista para a lagoa e mata nativa.",
  "endereco": "Florianópolis, SC",
  "createdAt": "2026-06-22T14:30:00.000Z",
  "updatedAt": "2026-06-22T14:30:00.000Z"
}
```

### Exemplo de requisição

```bash
curl -X POST http://localhost:3000/trilhas \
  -H "Content-Type: application/json" \
  -d '{
    "titulo": "Trilha da Lagoa",
    "descricao": "Percurso com vista para a lagoa e mata nativa.",
    "endereco": "Florianópolis, SC"
  }'
```

### Respostas de erro

Os erros seguem este formato:

```json
{
  "mensagem": "Título, descrição e endereço são obrigatórios."
}
```

Possíveis respostas incluem:

- `400` para corpo, dados ou ID inválidos;
- `404` para trilha ou rota não encontrada;
- `500` para erro interno do servidor.

## Scripts disponíveis

| Comando | Descrição |
| --- | --- |
| `npm run dev` | Inicia a API em modo de desenvolvimento |
| `npm start` | Inicia a API |
| `npm test` | Executa os testes e gera o relatório de cobertura |
| `npm run lint` | Verifica os arquivos de `src` com o ESLint |

## Estrutura do projeto

```text
src/
├── config/          # Conexão com o banco de dados
├── controllers/     # Regras das operações da API
├── middlewares/     # Validação e tratamento de erros
├── models/          # Modelos do Mongoose
├── routes/          # Definição dos endpoints
├── utils/           # Leitura dos parâmetros de consulta
├── app.js           # Configuração do Express
└── server.js        # Conexão ao banco e inicialização do servidor
tests/              # Testes automatizados
pipeline/           # Pipeline de Big Data da N3 (Airflow + Spark + MongoDB)
```

## Testes e qualidade

Execute a suíte de testes:

```bash
npm test
```

O relatório HTML de cobertura é gerado em `coverage/lcov-report/index.html`.

Para verificar o padrão do código:

```bash
npm run lint
```
