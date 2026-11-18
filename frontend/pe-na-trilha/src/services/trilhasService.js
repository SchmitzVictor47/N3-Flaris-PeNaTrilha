import api from "./api";

export async function listarTrilhas(config = {}) {
  const resposta = await api.get("/trilhas", config);
  return resposta.data;
}

// A API devolve a página pedida e o total de trilhas no cabeçalho X-Total-Count
export async function listarTrilhasPaginadas(
  { pagina = 1, limite = 20 } = {},
  config = {},
) {
  const resposta = await api.get("/trilhas", {
    ...config,
    params: { pagina, limite },
  });

  return {
    trilhas: resposta.data,
    total: Number(resposta.headers["x-total-count"] ?? resposta.data.length),
  };
}

export async function buscarTrilha(id) {
  const resposta = await api.get(`/trilhas/${id}`);
  return resposta.data;
}

export async function criarTrilha(dados) {
  const resposta = await api.post("/trilhas", dados);
  return resposta.data;
}

export async function atualizarTrilha(id, dados) {
  const resposta = await api.put(`/trilhas/${id}`, dados);
  return resposta.data;
}

export async function excluirTrilha(id) {
  await api.delete(`/trilhas/${id}`);
}
