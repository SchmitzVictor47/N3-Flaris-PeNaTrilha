import axios from "axios";
import { useEffect, useState } from "react";
import PostTrilha from "../components/PostTrilha";
import Sidebar from "../components/Sidebar";
import { obterMensagemErro } from "../services/api";
import {
  excluirTrilha,
  listarTrilhasPaginadas,
} from "../services/trilhasService";

const TRILHAS_POR_PAGINA = 20;

function ListaTrilhas() {
  const [trilhas, setTrilhas] = useState([]);
  const [total, setTotal] = useState(0);
  const [pagina, setPagina] = useState(1);
  const [carregando, setCarregando] = useState(true);
  const [carregandoMais, setCarregandoMais] = useState(false);
  const [erro, setErro] = useState("");
  const [idExcluindo, setIdExcluindo] = useState("");

  useEffect(() => {
    const controller = new AbortController();

    async function carregarTrilhas() {
      try {
        const resultado = await listarTrilhasPaginadas(
          { pagina: 1, limite: TRILHAS_POR_PAGINA },
          { signal: controller.signal },
        );
        setTrilhas(resultado.trilhas);
        setTotal(resultado.total);
        setErro("");
      } catch (erroRequisicao) {
        if (!axios.isCancel(erroRequisicao)) {
          setErro(obterMensagemErro(erroRequisicao));
        }
      } finally {
        setCarregando(false);
      }
    }

    carregarTrilhas();

    return () => controller.abort();
  }, []);

  const handleCarregarMais = async () => {
    try {
      setCarregandoMais(true);
      const proximaPagina = pagina + 1;
      const resultado = await listarTrilhasPaginadas({
        pagina: proximaPagina,
        limite: TRILHAS_POR_PAGINA,
      });

      setTrilhas((estadoAtual) => {
        const idsAtuais = new Set(estadoAtual.map((item) => item._id));
        return [
          ...estadoAtual,
          ...resultado.trilhas.filter((item) => !idsAtuais.has(item._id)),
        ];
      });
      setTotal(resultado.total);
      setPagina(proximaPagina);
      setErro("");
    } catch (erroRequisicao) {
      setErro(obterMensagemErro(erroRequisicao));
    } finally {
      setCarregandoMais(false);
    }
  };

  const handleExcluir = async (trilha) => {
    const confirmou = window.confirm(
      `Deseja realmente excluir a trilha "${trilha.titulo}"?`,
    );

    if (!confirmou) return;

    try {
      setIdExcluindo(trilha._id);
      await excluirTrilha(trilha._id);
      setTrilhas((estadoAtual) =>
        estadoAtual.filter((item) => item._id !== trilha._id),
      );
      setTotal((totalAtual) => Math.max(totalAtual - 1, 0));
      setErro("");
    } catch (erroRequisicao) {
      setErro(obterMensagemErro(erroRequisicao));
    } finally {
      setIdExcluindo("");
    }
  };

  return (
    <main className='layout'>
      <Sidebar trilhaCount={total} />

      <section className='feed' aria-live='polite'>
        {erro ? (
          <div className='storageAlert' role='alert'>
            {erro}
          </div>
        ) : null}

        {carregando ? (
          <div className='emptyFeed'>
            <strong>Carregando trilhas...</strong>
          </div>
        ) : null}

        {!carregando && trilhas.length === 0 ? (
          <div className='emptyFeed'>
            <strong>Nenhuma trilha cadastrada ainda.</strong>
            <span>Cadastre a primeira trilha para iniciar a lista.</span>
          </div>
        ) : null}

        {trilhas.map((trilha) => (
          <PostTrilha
            key={trilha._id}
            trilha={trilha}
            excluindo={idExcluindo === trilha._id}
            onDelete={() => handleExcluir(trilha)}
          />
        ))}

        {!carregando && trilhas.length < total ? (
          <button
            className='loadMoreButton'
            type='button'
            onClick={handleCarregarMais}
            disabled={carregandoMais}
          >
            {carregandoMais
              ? "Carregando..."
              : `Carregar mais trilhas (${trilhas.length.toLocaleString("pt-BR")} de ${total.toLocaleString("pt-BR")})`}
          </button>
        ) : null}
      </section>
    </main>
  );
}

export default ListaTrilhas;
