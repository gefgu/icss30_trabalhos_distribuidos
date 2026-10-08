const partes = window.location.pathname.split("/");
const pedidoId = partes[partes.length - 1];
const statusElement = document.getElementById("status");
const mensagemElement = document.getElementById("checkout-mensagem");
const botoes = [
    document.getElementById("botao-aprovar"),
    document.getElementById("botao-recusar"),
];

document.getElementById("pedido-id").textContent = pedidoId;

async function atualizarPagamento(decisao) {
    botoes.forEach((botao) => {
        botao.disabled = true;
    });
    mensagemElement.className = "mensagem";
    mensagemElement.textContent = "Enviando decisão...";

    try {
        const resposta = await fetch(`/app_pagamento/${pedidoId}/${decisao}`, {
            method: "POST",
        });
        const resultado = await resposta.json();
        if (!resposta.ok) throw new Error(resultado.detail || resultado.erro || "Falha ao processar pagamento.");

        statusElement.textContent = resultado.status;
        statusElement.className = `checkout-status ${resultado.status.toLowerCase()}`;
        mensagemElement.className = "mensagem ok";
        mensagemElement.textContent = "Decisão registrada com sucesso.";
    } catch (erro) {
        mensagemElement.className = "mensagem erro";
        mensagemElement.textContent = erro.message;
        botoes.forEach((botao) => {
            botao.disabled = false;
        });
    }
}

function aprovarPagamento() {
    return atualizarPagamento("aprovar");
}

function recusarPagamento() {
    return atualizarPagamento("recusar");
}