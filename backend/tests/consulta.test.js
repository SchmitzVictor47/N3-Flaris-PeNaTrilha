import { lerNumero, montarConsultaTrilhas } from "../src/utils/consulta.js";

describe("montarConsultaTrilhas", () => {
  test("usa limite 50 e página 1 quando não há parâmetros", () => {
    const consulta = montarConsultaTrilhas({});

    expect(consulta.filtro).toEqual({});
    expect(consulta.limite).toBe(50);
    expect(consulta.pagina).toBe(1);
    expect(consulta.pular).toBe(0);
  });

  test("monta os filtros de dificuldade, distância, fonte, país e busca", () => {
    const consulta = montarConsultaTrilhas({
      dificuldade: "3",
      distanciaMin: "5",
      distanciaMax: "10",
      fonte: "hikr.org",
      pais: "Suíça",
      busca: "Piz (Buin)",
      limite: "20",
      pagina: "3",
    });

    expect(consulta.filtro).toEqual({
      "dificuldade.grau": 3,
      "metricas.distancia_km": { $gte: 5, $lte: 10 },
      "origem.fonte": "hikr.org",
      "localizacao.pais": "Suíça",
      titulo: { $regex: "Piz \\(Buin\\)", $options: "i" },
    });
    expect(consulta.pular).toBe(40);
  });

  test("rejeita valores fora dos limites", () => {
    expect(() => montarConsultaTrilhas({ dificuldade: "7" })).toThrow(
      'O parâmetro "dificuldade" deve ser um número inteiro (mínimo 1, máximo 6).',
    );
    expect(() => montarConsultaTrilhas({ limite: "500" })).toThrow();
  });
});

describe("lerNumero", () => {
  test("ignora valores ausentes e converte números válidos", () => {
    expect(lerNumero(undefined, "x")).toBeUndefined();
    expect(lerNumero("", "x")).toBeUndefined();
    expect(lerNumero("-27.59", "lat", { min: -90, max: 90 })).toBe(-27.59);
  });

  test("rejeita texto que não é número", () => {
    expect(() => lerNumero("abc", "lat")).toThrow('O parâmetro "lat" deve ser um número.');
  });
});
