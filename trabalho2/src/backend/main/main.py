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

from fastapi import FastAPI
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from backend.main.consumer import iniciar_consumo
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


pedidos = []


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)
channel = connection.channel()
init_ecommerce_exchange(channel)


app = FastAPI()

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
    return {"produtos": "Lista de produtos disponíveis em estoque."}


@app.post("/pedido")
async def criar_pedido(pedido: Pedido):

    pedido_aprovado = False

    for item in pedido.pedidos:
        # Make request to the Estoque microservice to check stock availability

        pedido_aprovado = True
        pass

    if pedido_aprovado:
        # Publish the pedido.criado event to RabbitMQ
        novo_id_pedido = max([p["id"] for p in pedidos], default=0) + 1
        novo_pedido = {
            "id": novo_id_pedido,
            "produtos": [item.model_dump() for item in pedido.pedidos],
            "estoque": "disponível",
        }
        pedidos.append(novo_pedido)

        channel.basic_publish(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            routing_key="pedido.criado",
            body=str(novo_pedido),
        )

        return {"message": "Pedido criado com sucesso."}


@app.post("/interesse")
async def registrar_interesse():
    return {"message": "Interesse registrado com sucesso."}


@app.delete("/interesse")
async def cancelar_interesse():
    return {"message": "Interesse cancelado com sucesso."}


def main():
    consumer_thread = threading.Thread(target=iniciar_consumo, daemon=True)

    consumer_thread.start()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrompendo os consumidores...")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
