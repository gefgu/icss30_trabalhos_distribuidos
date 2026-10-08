# MS Principal / API Gateway (Valor: 1,8)
# Responsável por:
# • expor a API REST consumida pelo frontend;
# • transformar ações dos usuários em eventos publicados no RabbitMQ;
# • (0,1) publicar os eventos interesse.promocao, pedido.criado,
# pedido.excluído.
# • Consumir todos os eventos gerados pelo microsserviços Estoque,
# Pagamento e Entrega (pedido.estoque_ok, estoque.indisponivel,
# pagamento.aprovado, pagamento.recusado, pedido.enviado)
# • (0,5) manter conexões SSE com o frontend para envio de notificações
# aos clientes sobre o status do(s) seu(s) pedido(s). As notificações
# SSE devem incluir toda mudança de estado sobre o pedido: Estoque
# Confirmado, Pagamento Aprovado/Recusado, Pedido Enviado.
# O API Gateway deve disponibilizar endpoints REST para:
# • (0,1) listar produtos disponíveis em estoque;
# • (0,1) criar pedidos;
# • (0,1) registrar interesse, informando o e-mail, em receber notificação
# sobre promoções de categorias;
# • (0,1) cancelar interesse em receber e-mail sobre.
# Obs.: O API Gateway deve consultar a lista de produtos diretamente do MS
# Estoque (via REST).


import os
import sys
import threading
import pika
import httpx

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.sse import EventSourceResponse
import asyncio
import json
from pydantic import BaseModel
from contextlib import asynccontextmanager

from backend.main.consumer import iniciar_consumo
from backend.main.db import (
    criar_pedido as salvar_pedido,
    inicializar_banco,
    buscar_pedido,
    listar_pedidos as buscar_pedidos,
)
from helpers.helper import (
    EXCHANGE_ECOMMERCE_NAME,
    EXCHANGE_ECOMMERCE_NAME,
    init_ecommerce_exchange,
)


class PedidoItem(BaseModel):
    id: int
    nome: str
    categoria: str
    quantidade: int


class Pedido(BaseModel):
    pedidos: list[PedidoItem]


class Produto(BaseModel):
    id: int
    nome: str
    categoria: str
    estoque: int


class Interesse(BaseModel):
    email: str
    categoria: str  # A, B, C ou *


inicializar_banco()

ESTOQUE_API_URL = os.getenv("ESTOQUE_API_URL", "http://127.0.0.1:8001")
loop = None
listeners = {}
checkout_urls = {}

def publicar(routing_key, body):
    """
    Abre uma conexão curta com o RabbitMQ, publica e fecha.
    Uma conexão global parada morre por falta de heartbeat, e o pika
    não pode ser compartilhado entre threads.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    try:
        channel = connection.channel()
        init_ecommerce_exchange(channel)
        channel.basic_publish(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            routing_key=routing_key,
            body=body,
        )
    finally:
        connection.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global loop
    loop = asyncio.get_running_loop()
    consumer_thread = threading.Thread(
        target=iniciar_consumo, args=(notify_sse,), daemon=True
    )
    consumer_thread.start()
    yield
    print("Interrompendo os consumidores...")
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)


app = FastAPI(
    lifespan=lifespan,
    title="API Gateway",
    description="API Gateway do E-commerce",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"message": "API Gateway is running."}


@app.get("/produtos")
async def listar_produtos():
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resposta = await client.get(f"{ESTOQUE_API_URL}/produtos")
            resposta.raise_for_status()
            return resposta.json()
    except httpx.HTTPError as erro:
        raise HTTPException(
            status_code=503,
            detail="Não foi possível consultar os produtos no serviço de estoque.",
        ) from erro


@app.post("/pedido")
async def criar_pedido(pedido: Pedido):
    novo_pedido = {
        "produtos": [item.model_dump() for item in pedido.pedidos],
    }
    novo_pedido["id"] = salvar_pedido(novo_pedido)

    publicar("pedido.criado", str(novo_pedido))

    return {
        "message": "Pedido criado com sucesso.",
        "id": novo_pedido["id"],
    }


@app.post("/interesse")
async def registrar_interesse(interesse: Interesse):
    publicar("interesse.promocao", str(interesse.model_dump()))

    return {"message": "Interesse registrado com sucesso."}


@app.delete("/interesse")
async def cancelar_interesse(interesse: Interesse):
    publicar("interesse.cancelado", str(interesse.model_dump()))

    return {"message": "Interesse cancelado com sucesso."}


@app.get("/pedidos")
async def listar_pedidos():
    pedidos = buscar_pedidos()
    for pedido in pedidos:
        pedido["url_pagamento"] = checkout_urls.get(pedido["id"])
    return {"pedidos": pedidos}


@app.get("/pedidos/{pedido_id}/status")
async def obter_status_pedido(pedido_id: int):
    queue = asyncio.Queue()
    listeners.setdefault(pedido_id, set()).add(queue)

    async def stream():
        try:
            # Manda o que já aconteceu antes do cliente conectar
            # (o pedido pode ter avançado antes do frontend abrir o SSE).
            pedido = buscar_pedido(pedido_id)
            if pedido:
                for campo in ("estoque", "pagamento", "envio"):
                    if pedido[campo] != "pendente":
                        yield f"data: {json.dumps({'campo': campo, 'status': pedido[campo]})}\n\n"

            checkout_url = checkout_urls.get(pedido_id)
            if checkout_url:
                yield f"data: {json.dumps({'url': checkout_url})}\n\n"

            while True:
                status = await queue.get()
                yield f"data: {json.dumps(status)}\n\n"
        finally:
            queues = listeners.get(pedido_id)
            if queues is not None:
                queues.discard(queue)
            if queues is not None and not queues:
                del listeners[pedido_id]

    return EventSourceResponse(stream(), media_type="text/event-stream")


def notify_sse(pedido_id, status):
    def send():
        checkout_url = status.get("url")
        if checkout_url:
            checkout_urls[pedido_id] = checkout_url

        for queue in listeners.get(pedido_id, set()):
            queue.put_nowait(status)

    loop.call_soon_threadsafe(send)
