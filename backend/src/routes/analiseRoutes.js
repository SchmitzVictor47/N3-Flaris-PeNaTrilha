import { Router } from "express";
import {
  buscarTrilheiro,
  listarTrilheiros,
  perfilDificuldade,
  sazonalidade,
} from "../controllers/analiseController.js";

export const trilheiroRoutes = Router();
trilheiroRoutes.get("/", listarTrilheiros);
trilheiroRoutes.get("/:id", buscarTrilheiro);

export const analiseRoutes = Router();
analiseRoutes.get("/dificuldade", perfilDificuldade);
analiseRoutes.get("/sazonalidade", sazonalidade);
