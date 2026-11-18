import cors from "cors";
import express from "express";
import trilhaRoutes from "./routes/trilhaRoutes.js";
import { analiseRoutes, trilheiroRoutes } from "./routes/analiseRoutes.js";
import {
  rotaNaoEncontrada,
  tratarErros,
} from "./middlewares/errorHandler.js";

const app = express();

app.use(
  cors({
    origin: process.env.FRONTEND_URL || "http://localhost:5173",
    exposedHeaders: ["X-Total-Count", "X-Pagina", "X-Limite"],
  }),
);
app.use(express.json());

app.use("/trilhas", trilhaRoutes);
app.use("/trilheiros", trilheiroRoutes);
app.use("/analises", analiseRoutes);

app.use(rotaNaoEncontrada);
app.use(tratarErros);

export default app;
