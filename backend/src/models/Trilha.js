import mongoose from "mongoose";

// Ponto GeoJSON: coordinates = [longitude, latitude]
const pontoSchema = new mongoose.Schema(
  {
    type: { type: String, enum: ["Point"], default: "Point" },
    coordinates: { type: [Number], default: undefined },
  },
  { _id: false },
);

const trilhaSchema = new mongoose.Schema(
  {
    titulo: {
      type: String,
      required: [true, "O título é obrigatório."],
      trim: true,
    },
    descricao: {
      type: String,
      required: [true, "A descrição é obrigatória."],
      trim: true,
    },
    endereco: {
      type: String,
      required: [true, "O endereço é obrigatório."],
      trim: true,
    },
    // Campos preenchidos pelo pipeline de ingestão (pipeline/); opcionais no cadastro manual
    dificuldade: {
      codigo: String,
      grau: Number,
      modificador: String,
      nome: String,
    },
    localizacao: {
      inicio: { type: pontoSchema, default: undefined },
      cidade: String,
      regiao: String,
      pais: String,
    },
    metricas: {
      distancia_km: Number,
      subida_m: Number,
      descida_m: Number,
      altitude_min_m: Number,
      altitude_max_m: Number,
      tempo_movimento_min: Number,
      duracao_total_min: Number,
      velocidade_media_kmh: Number,
      velocidade_max_kmh: Number,
    },
    periodo: {
      inicio: Date,
      fim: Date,
    },
    trilheiro: {
      _id: String,
      usuario: String,
    },
    qualidade: {
      tem_tempo: Boolean,
      tem_elevacao: Boolean,
      alertas: { type: [String], default: undefined },
    },
    origem: {
      fonte: { type: String, default: "manual" },
      dataset: String,
      url: String,
      post_id: Number,
    },
  },
  {
    timestamps: true,
    versionKey: false,
  },
);

const Trilha = mongoose.model("Trilha", trilhaSchema);

export default Trilha;
