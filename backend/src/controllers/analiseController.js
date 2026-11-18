import mongoose from "mongoose";
import Trilha from "../models/Trilha.js";
import { lerNumero } from "../utils/consulta.js";

const MESES = [
  "janeiro",
  "fevereiro",
  "março",
  "abril",
  "maio",
  "junho",
  "julho",
  "agosto",
  "setembro",
  "outubro",
  "novembro",
  "dezembro",
];

// GET /trilheiros?limite=10 — ranking lido direto da coleção trilheiros
export async function listarTrilheiros(req, res, next) {
  try {
    const limite =
      lerNumero(req.query.limite, "limite", { min: 1, max: 100, inteiro: true }) ?? 10;

    const trilheiros = await mongoose.connection
      .collection("trilheiros")
      .find()
      .sort({ "estatisticas.total_trilhas": -1 })
      .limit(limite)
      .toArray();

    res.status(200).json(trilheiros);
  } catch (erro) {
    next(erro);
  }
}

export async function buscarTrilheiro(req, res, next) {
  try {
    const trilheiro = await mongoose.connection
      .collection("trilheiros")
      .findOne({ _id: req.params.id });

    if (!trilheiro) {
      return res.status(404).json({ mensagem: "Trilheiro não encontrado." });
    }

    return res.status(200).json(trilheiro);
  } catch (erro) {
    return next(erro);
  }
}

// GET /analises/dificuldade — perfil por grau SAC (consulta da N2)
export async function perfilDificuldade(_req, res, next) {
  try {
    const perfil = await Trilha.aggregate([
      {
        $match: { "qualidade.tem_tempo": true, "qualidade.tem_elevacao": true },
      },
      {
        $group: {
          _id: "$dificuldade.grau",
          trilhas: { $sum: 1 },
          distancia_media_km: { $avg: "$metricas.distancia_km" },
          subida_media_m: { $avg: "$metricas.subida_m" },
          velocidade_media_kmh: { $avg: "$metricas.velocidade_media_kmh" },
        },
      },
      { $sort: { _id: 1 } },
    ]);

    res.status(200).json(perfil);
  } catch (erro) {
    next(erro);
  }
}

// GET /analises/sazonalidade — trilhas por mês de início
export async function sazonalidade(_req, res, next) {
  try {
    const meses = await Trilha.aggregate([
      { $match: { "periodo.inicio": { $ne: null } } },
      { $group: { _id: { $month: "$periodo.inicio" }, trilhas: { $sum: 1 } } },
      { $sort: { _id: 1 } },
    ]);

    res
      .status(200)
      .json(meses.map(({ _id, trilhas }) => ({ mes: _id, nome: MESES[_id - 1], trilhas })));
  } catch (erro) {
    next(erro);
  }
}
