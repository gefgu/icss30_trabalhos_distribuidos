// =============================================================
// Frontend da Loja Distribuída
// Fala com o API Gateway por REST (fetch) e recebe o status dos
// pedidos em tempo real por SSE (EventSource).
// =============================================================

// Endereço do API Gateway (FastAPI).
const API_URL = "http://127.0.0.1:8000";

// ---------------------------------------------------------------- Estado
let produtos = [];           // catálogo carregado
const carrinho = new Map();  // id do produto -> quantidade
const pedidos = new Map();   // id do pedido  -> dados do pedido

// ---------------------------------------------------------------- Atalhos
const $ = (id) => document.getElementById(id);

// Evita que um texto vindo da API seja interpretado como HTML.
function escapar(texto) {
  const div = document.createElement("div");
  div.textContent = String(texto);
  return div.innerHTML;
}

function mostrarMensagem(elemento, texto, tipo) {
  elemento.textContent = texto;
  elemento.className = `mensagem ${tipo}`;
}

// =============================================================
// 1. CATÁLOGO  (GET /produtos)
// =============================================================
async function carregarProdutos() {
  const aviso = $("aviso-catalogo");
  aviso.hidden = true;

  try {
    const resposta = await fetch(`${API_URL}/produtos`);
    if (!resposta.ok) throw new Error(`Gateway respondeu ${resposta.status}`);

    const dados = await resposta.json();
    if (!Array.isArray(dados.produtos)) {
      throw new Error("Resposta do Gateway não contém uma lista de produtos.");
    }

    produtos = dados.produtos;
    if (produtos.length === 0) {
      mostrarAvisoCatalogo("Não há produtos disponíveis em estoque.");
    }
  } catch (erro) {
    produtos = [];
    mostrarAvisoCatalogo(`Não foi possível consultar os produtos: ${erro.message}`);
  }
  renderizarProdutos();
}

function mostrarAvisoCatalogo(texto) {
  const aviso = $("aviso-catalogo");
  aviso.textContent = texto;
  aviso.hidden = false;
}

function renderizarProdutos() {
  $("lista-produtos").innerHTML = produtos
    .map((p) => {
      const noCarrinho = carrinho.get(p.id) || 0;
      const semEstoque = p.estoque - noCarrinho <= 0;
      return `
        <li class="produto">
          <span class="selo" aria-hidden="true">${escapar(p.categoria)}</span>
          <div>
            <div class="produto-nome">${escapar(p.nome)}</div>
            <div class="produto-estoque ${p.estoque === 0 ? "zerado" : ""}">
              ${p.estoque === 0 ? "Sem estoque" : `${p.estoque} em estoque`} · categoria ${escapar(p.categoria)}
            </div>
          </div>
          <button class="botao secundario" data-adicionar="${p.id}" ${semEstoque ? "disabled" : ""}>
            Adicionar
          </button>
        </li>`;
    })
    .join("");
}

// Um único "ouvinte" na lista inteira, em vez de um por botão.
$("lista-produtos").addEventListener("click", (evento) => {
  const botao = evento.target.closest("[data-adicionar]");
  if (!botao) return;
  adicionarAoCarrinho(Number(botao.dataset.adicionar));
});

// =============================================================
// 2. CARRINHO E CRIAÇÃO DO PEDIDO  (POST /pedido)
// =============================================================
function adicionarAoCarrinho(idProduto) {
  carrinho.set(idProduto, (carrinho.get(idProduto) || 0) + 1);
  renderizarCarrinho();
  renderizarProdutos();
}

function removerDoCarrinho(idProduto) {
  carrinho.delete(idProduto);
  renderizarCarrinho();
  renderizarProdutos();
}

function renderizarCarrinho() {
  const itens = [...carrinho.entries()];
  $("itens-carrinho").innerHTML = itens
    .map(([id, quantidade]) => {
      const produto = produtos.find((p) => p.id === id);
      return `
        <li class="item">
          <span>${quantidade}× ${escapar(produto.nome)}</span>
          <button data-remover="${id}">remover</button>
        </li>`;
    })
    .join("");

  $("carrinho-vazio").hidden = itens.length > 0;
  $("botao-finalizar").disabled = itens.length === 0;
}

$("itens-carrinho").addEventListener("click", (evento) => {
  const botao = evento.target.closest("[data-remover]");
  if (botao) removerDoCarrinho(Number(botao.dataset.remover));
});

$("botao-finalizar").addEventListener("click", finalizarPedido);

async function finalizarPedido() {
  const mensagem = $("msg-carrinho");
  const botao = $("botao-finalizar");
  const abaCheckout = window.open("about:blank", "_blank");
  if (abaCheckout) {
    abaCheckout.document.write("<title>Checkout</title><p>Aguardando confirmação do estoque e criação do pagamento...</p>");
    abaCheckout.document.close();
  }

  // Monta o corpo no formato que o Gateway espera (modelo Pedido).
  const itens = [...carrinho.entries()].map(([id, quantidade]) => {
    const p = produtos.find((produto) => produto.id === id);
    return { id: p.id, nome: p.nome, categoria: p.categoria, quantidade };
  });

  botao.disabled = true;
  try {
    const resposta = await fetch(`${API_URL}/pedido`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pedidos: itens }),
    });
    if (!resposta.ok) throw new Error(`Gateway respondeu ${resposta.status}`);
    const dados = await resposta.json();

    // Guarda o pedido e já começa a escutar o status dele.
    pedidos.set(dados.id, {
      id: dados.id,
      itens,
      estoque: "pendente",
      pagamento: "pendente",
      envio: "pendente",
      urlPagamento: null,
      abaCheckout,
      conexao: null,
    });
    acompanharPedido(dados.id);

    carrinho.clear();
    renderizarCarrinho();
    renderizarProdutos();
    renderizarPedidos();
    mostrarMensagem(mensagem, `Pedido #${dados.id} criado. Acompanhe abaixo.`, "ok");
  } catch (erro) {
    if (abaCheckout && !abaCheckout.closed) abaCheckout.close();
    botao.disabled = false;
    mostrarMensagem(mensagem, `Não foi possível criar o pedido: ${erro.message}.`, "erro");
  }
}

// =============================================================
// 3. ACOMPANHAMENTO EM TEMPO REAL  (SSE)
// =============================================================
function acompanharPedido(idPedido) {
  // Abre uma conexão que fica aberta: o Gateway empurra cada mudança.
  const conexao = new EventSource(`${API_URL}/pedidos/${idPedido}/status`);
  pedidos.get(idPedido).conexao = conexao;

  // Cada evento chega como: data: {"campo": "estoque", "status": "ok"}
  conexao.onmessage = (evento) => {
    const atualizacao = JSON.parse(evento.data);
    atualizarPedido(idPedido, atualizacao);
  };

  conexao.onerror = () => {
    // O EventSource tenta reconectar sozinho; só registramos no console.
    console.warn(`SSE do pedido ${idPedido} caiu, tentando reconectar...`);
  };
}

function atualizarPedido(idPedido, atualizacao) {
  const pedido = pedidos.get(idPedido);
  if (!pedido) return;

  // campo = "estoque" | "pagamento" | "envio"
  if (atualizacao.campo) pedido[atualizacao.campo] = atualizacao.status;

  if (atualizacao.url) {
    pedido.urlPagamento = atualizacao.url;
    if (pedido.abaCheckout && !pedido.abaCheckout.closed) {
      pedido.abaCheckout.location.href = atualizacao.url;
      pedido.abaCheckout = null;
    }
  }

  if (
    atualizacao.campo === "estoque" &&
    atualizacao.status === "indisponível" &&
    pedido.abaCheckout &&
    !pedido.abaCheckout.closed
  ) {
    pedido.abaCheckout.close();
    pedido.abaCheckout = null;
  }

  // Quando o pedido termina (bem ou mal), fecha a conexão SSE.
  if (pedidoTerminou(pedido)) pedido.conexao.close();

  renderizarPedidos();
}

function pedidoTerminou(pedido) {
  return (
    pedido.envio === "enviado" ||
    pedido.estoque === "indisponível" ||
    pedido.pagamento === "recusado"
  );
}

// Traduz o estado de cada etapa para texto + cor da bolinha.
function descreverEtapas(pedido) {
  const etapas = [];

  // Etapa 1: Estoque
  if (pedido.estoque === "ok") etapas.push(["Estoque", "Confirmado", "ok"]);
  else if (pedido.estoque === "indisponível") etapas.push(["Estoque", "Indisponível", "erro"]);
  else etapas.push(["Estoque", "Verificando", "andamento"]);

  // Etapa 2: Pagamento
  if (pedido.estoque === "indisponível") etapas.push(["Pagamento", "Cancelado", ""]);
  else if (pedido.pagamento === "aprovado") etapas.push(["Pagamento", "Aprovado", "ok"]);
  else if (pedido.pagamento === "recusado") etapas.push(["Pagamento", "Recusado", "erro"]);
  else if (pedido.estoque === "ok") etapas.push(["Pagamento", "Aguardando pagamento", "andamento"]);
  else etapas.push(["Pagamento", "Na fila", ""]);

  // Etapa 3: Envio
  if (pedido.envio === "enviado") etapas.push(["Envio", "Enviado", "ok"]);
  else if (pedido.estoque === "indisponível" || pedido.pagamento === "recusado") etapas.push(["Envio", "Cancelado", ""]);
  else if (pedido.pagamento === "aprovado") etapas.push(["Envio", "Preparando", "andamento"]);
  else etapas.push(["Envio", "Na fila", ""]);

  return etapas;
}

function renderizarPedidos() {
  const lista = [...pedidos.values()].reverse(); // mais recente primeiro
  $("sem-pedidos").hidden = lista.length > 0;

  $("lista-pedidos").innerHTML = lista
    .map((pedido) => {
      const resumo = pedido.itens.map((i) => `${i.quantidade}× ${escapar(i.nome)}`).join(", ");
      const etapas = descreverEtapas(pedido)
        .map(([nome, status, classe]) => `
          <li class="etapa ${classe}">
            <span class="etapa-nome">${nome}</span>
            <span class="etapa-status">${status}</span>
          </li>`)
        .join("");

      // Botão só aparece quando o Pagamento já mandou a URL do Mock.
      const podePagar = pedido.urlPagamento && pedido.pagamento === "pendente";
      const botaoPagar = podePagar
        ? `<button class="botao principal pagar" data-pagar="${pedido.id}">Pagar pedido</button>`
        : "";
      const botaoNovoPedido = pedido.envio === "enviado"
        ? `<button class="botao secundario novo-pedido" data-novo-pedido>Fazer novo pedido</button>`
        : "";
      const botaoRefazerPedido = pedido.pagamento === "recusado" && !pedido.itensRestaurados
        ? `<button class="botao secundario refazer-pedido" data-refazer-pedido="${pedido.id}">Refazer pedido</button>`
        : "";

      return `
        <li class="pedido">
          <div class="pedido-topo">
            <span class="pedido-titulo">Pedido #${pedido.id}</span>
            <span class="pedido-itens">${resumo}</span>
          </div>
          <ol class="trilha">${etapas}</ol>
          ${botaoPagar}
          ${botaoNovoPedido}
          ${botaoRefazerPedido}
        </li>`;
    })
    .join("");
}

async function carregarHistoricoPedidos() {
  try {
    const resposta = await fetch(`${API_URL}/pedidos`);
    if (!resposta.ok) throw new Error(`Gateway respondeu ${resposta.status}`);

    const dados = await resposta.json();
    for (const pedidoSalvo of dados.pedidos || []) {
      if (pedidos.has(pedidoSalvo.id)) continue;

      const pedido = {
        id: pedidoSalvo.id,
        itens: pedidoSalvo.produtos,
        estoque: pedidoSalvo.estoque,
        pagamento: pedidoSalvo.pagamento,
        envio: pedidoSalvo.envio,
        urlPagamento: pedidoSalvo.url_pagamento,
        abaCheckout: null,
        conexao: null,
      };
      pedidos.set(pedido.id, pedido);
    }

    for (const pedido of pedidos.values()) {
      if (!pedido.conexao && !pedidoTerminou(pedido)) acompanharPedido(pedido.id);
    }
    renderizarPedidos();
  } catch (erro) {
    console.error("Não foi possível carregar o histórico de pedidos:", erro);
  }
}

// Abre o Mock de Pagamento numa nova aba.
$("lista-pedidos").addEventListener("click", (evento) => {
  const botaoRefazer = evento.target.closest("[data-refazer-pedido]");
  if (botaoRefazer) {
    const pedido = pedidos.get(Number(botaoRefazer.dataset.refazerPedido));
    if (!pedido || pedido.itensRestaurados) return;

    pedido.itens.forEach((item) => {
      carrinho.set(item.id, (carrinho.get(item.id) || 0) + item.quantidade);
    });
    pedido.itensRestaurados = true;
    renderizarCarrinho();
    renderizarProdutos();
    renderizarPedidos();
    mostrarMensagem($("msg-carrinho"), `Itens do pedido #${pedido.id} foram restaurados no carrinho.`, "ok");
    return;
  }

  const botaoNovoPedido = evento.target.closest("[data-novo-pedido]");
  if (botaoNovoPedido) {
    window.location.reload();
    return;
  }

  const botao = evento.target.closest("[data-pagar]");
  if (!botao) return;
  const pedido = pedidos.get(Number(botao.dataset.pagar));
  window.open(pedido.urlPagamento, "_blank");
});

// =============================================================
// 4. PROMOÇÕES  (POST /interesse e DELETE /interesse)
// =============================================================
async function enviarInteresse(metodo) {
  const mensagem = $("msg-promocoes");
  const email = $("email").value.trim();
  const categoria = $("categoria").value;

  if (!$("email").checkValidity()) {
    mostrarMensagem(mensagem, "Digite um e-mail válido.", "erro");
    return;
  }

  try {
    const resposta = await fetch(`${API_URL}/interesse`, {
      method: metodo,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, categoria }),
    });
    if (!resposta.ok) throw new Error(`Gateway respondeu ${resposta.status}`);

    const nomeCategoria = categoria === "*" ? "todas as categorias" : `categoria ${categoria}`;
    const texto = metodo === "POST"
      ? `Pronto! ${email} vai receber promoções de ${nomeCategoria}.`
      : `Pronto! ${email} não vai mais receber promoções de ${nomeCategoria}.`;
    mostrarMensagem(mensagem, texto, "ok");
  } catch (erro) {
    mostrarMensagem(mensagem, `Não foi possível salvar: ${erro.message}.`, "erro");
  }
}

$("form-interesse").addEventListener("submit", (evento) => {
  evento.preventDefault(); // impede o navegador de recarregar a página
  enviarInteresse("POST");
});

$("botao-cancelar").addEventListener("click", () => enviarInteresse("DELETE"));

// ---------------------------------------------------------------- Início
carregarProdutos();
carregarHistoricoPedidos();
renderizarCarrinho();
renderizarPedidos();
