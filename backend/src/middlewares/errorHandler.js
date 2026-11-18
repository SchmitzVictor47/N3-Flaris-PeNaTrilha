export function rotaNaoEncontrada(req, res) {
  res.status(404).json({
    mensagem: `Rota ${req.method} ${req.originalUrl} não encontrada.`,
  });
}

// O Express só reconhece um middleware de erro quando ele declara os 4 parâmetros
// eslint-disable-next-line no-unused-vars
export function tratarErros(erro, _req, res, _next) {
  if (erro.type === "entity.parse.failed") {
    return res.status(400).json({ mensagem: "O JSON enviado é inválido." });
  }

  if (erro.name === "ErroConsulta") {
    return res.status(400).json({ mensagem: erro.message });
  }

  if (erro.name === "ValidationError") {
    const mensagens = Object.values(erro.errors).map(
      (detalhe) => detalhe.message,
    );

    return res.status(400).json({
      mensagem: "Dados da trilha inválidos.",
      erros: mensagens,
    });
  }

  if (erro.name === "CastError") {
    return res.status(400).json({ mensagem: "ID de trilha inválido." });
  }

  // $geoNear sem o índice 2dsphere (o pipeline de ingestão ainda não rodou)
  if (erro.code === 291 || erro.codeName === "IndexNotFound") {
    return res.status(400).json({
      mensagem:
        "Busca por proximidade indisponível: execute o pipeline de ingestão para criar o índice geoespacial.",
    });
  }

  console.error(erro);
  return res.status(500).json({ mensagem: "Erro interno do servidor." });
}
