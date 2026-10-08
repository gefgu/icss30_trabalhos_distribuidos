import httpx
import ast
import json
import pika
import uvicorn
import threading
from pathlib import Path
from fastapi import FastAPI

from helpers.helper import (
    EXCHANGE_ECOMMERCE_NAME,
    init_ecommerce_exchange,
)

FILE_FOLDER_PATH = Path(__file__).resolve().parents[0]
app = FastAPI()


def _parse_mensagem(body):
    if isinstance(body, (bytes, bytearray)):
        conteudo = body.decode("utf-8")
    else:
        conteudo = str(body)

    conteudo = conteudo.strip()
    if not conteudo or conteudo in {"None", "null"}:
        return {}

    try:
        return json.loads(conteudo)
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(conteudo)
        except (ValueError, SyntaxError):
            return {"id": conteudo}


def receber_mensagem(ch, method, properties, body):
    pedido = _parse_mensagem(body)

    if method.routing_key == "pedido.estoque_ok":
        pagamento = processar_pagamento(pedido)
        checkout_url = pagamento.get("checkout_url")
        if checkout_url:
            print(f"[PAGAMENTO] Checkout do pedido {pedido['id']}: {checkout_url}")
            ch.basic_publish(
                exchange=EXCHANGE_ECOMMERCE_NAME,
                routing_key="pagamento.checkout_criado",
                body=json.dumps({"id": pedido["id"], "url": checkout_url}),
            )

    ch.basic_ack(
        delivery_tag=method.delivery_tag
    )


def processar_pagamento(pedido):
    pedido_id = pedido["id"]

    with httpx.Client() as client:

        resposta = client.post(
            "http://localhost:8003/app_pagamento",
            params={
                "pedido_id": pedido_id,
                "webhook_url": "http://localhost:8002/webhook/pagamento"
            }
        )

    resposta.raise_for_status()
    return resposta.json()

@app.post("/webhook/pagamento")
async def receber_webhook(dados: dict):
    pedido_id = dados["pedido_id"]
    status = dados["status"]

    publicar_evento(
        pedido_id,
        status
    )

    return {"recebido": True}


def publicar_evento(pedido_id, status):
    body = json.dumps({"id": pedido_id})

    routing_key = (
        "pagamento.aprovado"
        if status == "APROVADO"
        else "pagamento.recusado"
    )

    channel.basic_publish(
        exchange=EXCHANGE_ECOMMERCE_NAME,
        routing_key=routing_key,
        body=body
    )

    print(
        f"[PAGAMENTO] Publicado: "
        f"{routing_key} - pedido {pedido_id}"
    )


if __name__ == "__main__":
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    init_ecommerce_exchange(channel)

    queue_name = "pagamento"
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(
        exchange=EXCHANGE_ECOMMERCE_NAME,
        queue=queue_name,
        routing_key="pedido.estoque_ok",
    )

    threading.Thread(
            target=lambda: uvicorn.run(app, host="127.0.0.1", port=8002),
            daemon=True,
        ).start()

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=receber_mensagem,
        auto_ack=False,
    )

    print("[PAGAMENTO] Aguardando eventos do RabbitMQ...")
    channel.start_consuming()
