import mongoose from "mongoose";
import Trilha from "../models/Trilha.js";
import { lerNumero, montarConsultaTrilhas } from "../utils/consulta.js";

export async function listarTrilhas(req, res, next) {
  try {
    const { filtro, limite, pagina, pular, ordenacao } = montarConsultaTrilhas(
      req.query,
    );

    const [trilhas, total] = await Promise.all([
      Trilha.find(filtro).sort(ordenacao).skip(pular).limit(limite),
      Trilha.countDocuments(filtro),
    ]);

    res.set({
      "X-Total-Count": String(total),
      "X-Pagina": String(pagina),
      "X-Limite": String(limite),
    });
    res.status(200).json(trilhas);
  } catch (erro) {
    next(erro);
  }
}

// GET /trilhas/proximas?lat=46.02&lon=7.75&raioKm=10&limite=10
export async function listarTrilhasProximas(req, res, next) {
  try {
    const lat = lerNumero(req.query.lat, "lat", { min: -90, max: 90 });
    const lon = lerNumero(req.query.lon, "lon", { min: -180, max: 180 });

    if (lat === undefined || lon === undefined) {
      return res
        .status(400)
        .json({ mensagem: 'Informe os parâmetros "lat" e "lon".' });
    }

    const raioKm = lerNumero(req.query.raioKm, "raioKm", { min: 0.1, max: 500 }) ?? 10;
    const limite =
      lerNumero(req.query.limite, "limite", { min: 1, max: 100, inteiro: true }) ?? 10;

    const trilhas = await Trilha.aggregate([
      {
        $geoNear: {
          near: { type: "Point", coordinates: [lon, lat] },
          distanceField: "distanciaAtePontoM",
          maxDistance: raioKm * 1000,
          spherical: true,
        },
      },
      { $limit: limite },
    ]);

    return res.status(200).json(trilhas);
  } catch (erro) {
    return next(erro);
  }
}

export async function buscarTrilha(req, res, next) {
  try {
    const trilha = await Trilha.findById(req.params.id);

    if (!trilha) {
      return res.status(404).json({ mensagem: "Trilha não encontrada." });
    }

    return res.status(200).json(trilha);
  } catch (erro) {
    return next(erro);
  }
}

// Pontos GPS gravados (coleção percursos, mesmo _id da trilha)
export async function buscarPercurso(req, res, next) {
  try {
    if (!mongoose.isValidObjectId(req.params.id)) {
      return res.status(400).json({ mensagem: "ID de trilha inválido." });
    }

    const percurso = await mongoose.connection
      .collection("percursos")
      .findOne({ _id: new mongoose.Types.ObjectId(req.params.id) });

    if (!percurso) {
      return res
        .status(404)
        .json({ mensagem: "Percurso não encontrado para esta trilha." });
    }

    return res.status(200).json(percurso);
  } catch (erro) {
    return next(erro);
  }
}

export async function criarTrilha(req, res, next) {
  try {
    const trilha = await Trilha.create(req.body);
    res.status(201).json(trilha);
  } catch (erro) {
    next(erro);
  }
}

export async function atualizarTrilha(req, res, next) {
  try {
    const trilha = await Trilha.findByIdAndUpdate(req.params.id, req.body, {
      new: true,
      runValidators: true,
    });

    if (!trilha) {
      return res.status(404).json({ mensagem: "Trilha não encontrada." });
    }

    return res.status(200).json(trilha);
  } catch (erro) {
    return next(erro);
  }
}

export async function excluirTrilha(req, res, next) {
  try {
    const trilha = await Trilha.findByIdAndDelete(req.params.id);

    if (!trilha) {
      return res.status(404).json({ mensagem: "Trilha não encontrada." });
    }

    return res.status(204).send();
  } catch (erro) {
    return next(erro);
  }
}
