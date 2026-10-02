# Consome pedido.estoque_ok.
# • (0,1) Solicita a criação de uma cobrança ao Mock de Pagamento para
# este gerar a URL de checkout, passando a sua URL de Webhook para
# retorno.
# • (0,1) Disponibiliza o endpoint HTTP para receber o Webhook do
# Mock de Pagamento com o status APROVADO ou RECUSADO.
# • Publica no RabbitMQ os
# eventos pagamento.aprovado ou pagamento.recusado.


import random
import ast
import json
import pika
from pathlib import Path

from helpers.helper import (
    EXCHANGE_ECOMMERCE_NAME,
    init_ecommerce_exchange,
)

FILE_FOLDER_PATH = Path(__file__).resolve().parents[0]


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


def processar_pagamento(pedido):
    dados = pedido if isinstance(pedido, dict) else _parse_mensagem(pedido)
    pedido_id = dados.get("id") if isinstance(dados, dict) else dados

    resultado = random.choice(["aprovado", "recusado"])
    routing_key = (
        "pagamento.aprovado" if resultado == "aprovado" else "pagamento.recusado"
    )
    return {"id": pedido_id, "status": resultado, "routing_key": routing_key}


def receber_mensagem(ch, method, properties, body):
    pedido = _parse_mensagem(body)
    routing_key = method.routing_key

    if routing_key == "pedido.estoque_ok":

        resultado = processar_pagamento(pedido)
        pedido_id = resultado["id"]

        # publish as plain string and put signature in header
        payload = {"id": pedido_id}
        body_out = str(payload)

        if resultado["status"] == "aprovado":
            ch.basic_publish(
                exchange=EXCHANGE_ECOMMERCE_NAME,
                routing_key="pagamento.aprovado",
                body=body_out,
            )
            print(f"[PAGAMENTO] Pedido {pedido_id} -> {resultado['status']}")
        else:
            ch.basic_publish(
                exchange=EXCHANGE_ECOMMERCE_NAME,
                routing_key="pagamento.recusado",
                body=body_out,
            )
            print(f"[PAGAMENTO] Pedido {pedido_id} -> {resultado['status']}")

        ch.basic_ack(delivery_tag=method.delivery_tag)


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

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=receber_mensagem,
        auto_ack=False,
    )

    print("[PAGAMENTO] Aguardando eventos do RabbitMQ...")
    channel.start_consuming()
