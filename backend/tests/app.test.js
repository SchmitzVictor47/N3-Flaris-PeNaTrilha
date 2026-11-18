import request from "supertest";
import app from "../src/app.js";

// Casos que respondem antes de qualquer acesso ao banco
describe("API — validação de parâmetros", () => {
  test("parâmetro inválido em GET /trilhas devolve 400 em JSON", async () => {
    const resposta = await request(app).get("/trilhas?limite=abc");

    expect(resposta.status).toBe(400);
    expect(resposta.body).toEqual({
      mensagem: 'O parâmetro "limite" deve ser um número inteiro (mínimo 1, máximo 100).',
    });
  });

  test("GET /trilhas/proximas exige lat e lon", async () => {
    const resposta = await request(app).get("/trilhas/proximas?lat=46");

    expect(resposta.status).toBe(400);
    expect(resposta.body.mensagem).toBe('Informe os parâmetros "lat" e "lon".');
  });

  test("GET /trilhas/:id/percurso rejeita ID inválido", async () => {
    const resposta = await request(app).get("/trilhas/abc/percurso");

    expect(resposta.status).toBe(400);
    expect(resposta.body.mensagem).toBe("ID de trilha inválido.");
  });

  test("rota inexistente devolve 404", async () => {
    const resposta = await request(app).get("/nao-existe");

    expect(resposta.status).toBe(404);
  });
});
