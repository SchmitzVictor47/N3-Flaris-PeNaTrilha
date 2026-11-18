import { Link } from "react-router-dom";

function EditIcon() {
  return (
    <svg viewBox='0 0 24 24' aria-hidden='true'>
      <path
        d='m4 16.5-.5 4 4-.5L19 8.5 15.5 5 4 16.5Zm9.8-9.8 3.5 3.5'
        fill='none'
        stroke='currentColor'
        strokeLinecap='round'
        strokeLinejoin='round'
        strokeWidth='1.8'
      />
    </svg>
  );
}

function TrashIcon() {
  return (
    <svg viewBox='0 0 24 24' aria-hidden='true'>
      <path
        d='M9 3.75h6a1 1 0 0 1 1 1V6h3.25a.75.75 0 0 1 0 1.5h-1.02l-.82 10.6A2.25 2.25 0 0 1 15.17 20H8.83a2.25 2.25 0 0 1-2.24-1.9L5.77 7.5H4.75a.75.75 0 0 1 0-1.5H8V4.75a1 1 0 0 1 1-1Zm5.5 2.25v-.75h-5V6h5Zm-6.4 1.5.78 10.32a.75.75 0 0 0 .75.68h4.74a.75.75 0 0 0 .75-.68l.78-10.32H8.1Zm2.15 2.35a.75.75 0 0 1 .75.75v4.8a.75.75 0 0 1-1.5 0v-4.8a.75.75 0 0 1 .75-.75Zm3.5 0a.75.75 0 0 1 .75.75v4.8a.75.75 0 0 1-1.5 0v-4.8a.75.75 0 0 1 .75-.75Z'
        fill='none'
        stroke='currentColor'
        strokeLinecap='round'
        strokeLinejoin='round'
        strokeWidth='1.8'
      />
    </svg>
  );
}

function formatarNumero(valor, casas = 0) {
  return valor.toLocaleString("pt-BR", {
    minimumFractionDigits: casas,
    maximumFractionDigits: casas,
  });
}

// Etiquetas com os dados que o pipeline de ingestão acrescenta às trilhas importadas
function montarEtiquetas(trilha) {
  const { dificuldade, metricas } = trilha;

  return [
    dificuldade?.codigo ? `${dificuldade.codigo} · ${dificuldade.nome}` : null,
    metricas?.distancia_km != null
      ? `${formatarNumero(metricas.distancia_km, 1)} km`
      : null,
    metricas?.subida_m != null ? `↑ ${formatarNumero(metricas.subida_m)} m` : null,
  ].filter(Boolean);
}

function PostTrilha({ trilha, onDelete, excluindo }) {
  const autorImportado = trilha.trilheiro?.usuario;
  const etiquetas = montarEtiquetas(trilha);

  return (
    <article className='post'>
      <header className='postHeader'>
        <div className='profileGroup'>
          {autorImportado ? (
            <div className='avatarWithBorderPost avatarInicial' aria-hidden='true'>
              {autorImportado.charAt(0).toUpperCase()}
            </div>
          ) : (
            <div className='avatarWithBorderPost'>
              <img
                src='https://github.com/DaviLLi.png'
                className='avatarPost'
                alt='Foto de Davi Martinelli'
              />
            </div>
          )}

          <div className='userInfo'>
            <h2>{autorImportado ?? "Davi Martinelli"}</h2>
            <span>{autorImportado ? "Trilheiro · hikr.org" : "Trilheiro"}</span>
          </div>
        </div>

        <div className='actionButtons'>
          <Link
            className='editBtn'
            to={`/trilhas/${trilha._id}/editar`}
            aria-label={`Editar trilha ${trilha.titulo}`}
            title='Editar trilha'
          >
            <EditIcon />
          </Link>
          <button
            className='deleteBtn'
            type='button'
            onClick={onDelete}
            disabled={excluindo}
            aria-label={`Excluir trilha ${trilha.titulo}`}
            title='Excluir trilha'
          >
            <TrashIcon />
          </button>
        </div>
      </header>

      <h3 className='postTitle'>{trilha.titulo}</h3>
      {etiquetas.length > 0 ? (
        <ul className='postTags'>
          {etiquetas.map((etiqueta) => (
            <li key={etiqueta}>{etiqueta}</li>
          ))}
        </ul>
      ) : null}
      <p className='postText'>{trilha.descricao}</p>
      <span className='postAddress'>{trilha.endereco}</span>
    </article>
  );
}

export default PostTrilha;
