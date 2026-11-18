export class ErroConsulta extends Error {
  constructor(mensagem) {
    super(mensagem);
    this.name = "ErroConsulta";
  }
}

export function lerNumero(valor, nome, { min, max, inteiro = false } = {}) {
  if (valor === undefined || valor === "") return undefined;

  const numero = Number(valor);
  const invalido =
    !Number.isFinite(numero) ||
    (inteiro && !Number.isInteger(numero)) ||
    (min !== undefined && numero < min) ||
    (max !== undefined && numero > max);

  if (invalido) {
    const limites = [
      min !== undefined ? `mínimo ${min}` : null,
      max !== undefined ? `máximo ${max}` : null,
    ]
      .filter(Boolean)
      .join(", ");
    throw new ErroConsulta(
      `O parâmetro "${nome}" deve ser um número${inteiro ? " inteiro" : ""}${limites ? ` (${limites})` : ""}.`,
    );
  }

  return numero;
}

function escaparRegex(texto) {
  return texto.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// Converte a query string de GET /trilhas em filtro, paginação e ordenação
export function montarConsultaTrilhas(query = {}) {
  const filtro = {};

  const dificuldade = lerNumero(query.dificuldade, "dificuldade", {
    min: 1,
    max: 6,
    inteiro: true,
  });
  if (dificuldade !== undefined) filtro["dificuldade.grau"] = dificuldade;

  const distanciaMin = lerNumero(query.distanciaMin, "distanciaMin", { min: 0 });
  const distanciaMax = lerNumero(query.distanciaMax, "distanciaMax", { min: 0 });
  if (distanciaMin !== undefined || distanciaMax !== undefined) {
    filtro["metricas.distancia_km"] = {};
    if (distanciaMin !== undefined) filtro["metricas.distancia_km"].$gte = distanciaMin;
    if (distanciaMax !== undefined) filtro["metricas.distancia_km"].$lte = distanciaMax;
  }

  if (typeof query.fonte === "string" && query.fonte.trim()) {
    filtro["origem.fonte"] = query.fonte.trim();
  }

  if (typeof query.pais === "string" && query.pais.trim()) {
    filtro["localizacao.pais"] = query.pais.trim();
  }

  if (typeof query.busca === "string" && query.busca.trim()) {
    filtro.titulo = { $regex: escaparRegex(query.busca.trim()), $options: "i" };
  }

  const limite =
    lerNumero(query.limite, "limite", { min: 1, max: 100, inteiro: true }) ?? 50;
  const pagina = lerNumero(query.pagina, "pagina", { min: 1, inteiro: true }) ?? 1;

  return {
    filtro,
    limite,
    pagina,
    pular: (pagina - 1) * limite,
    ordenacao: { createdAt: -1, _id: -1 },
  };
}
