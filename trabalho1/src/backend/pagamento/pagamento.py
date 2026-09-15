# O Microsserviço Pagamento é responsável pelo processamento dos pagamentos dos
# pedidos.
# O serviço deverá consumir o evento pedido.estoque_ok. Ao receber esse evento,
# deverá iniciar o processo de pagamento para o pedido correspondente.
# O processamento do pagamento deverá ser simulado por meio do uso de variáveis
# aleatórias para determinar se o pagamento será aprovado ou recusado.
# Quando o pagamento for aprovado, o microsserviço Pagamento deverá publicar um
# evento utilizando a routing key pagamento.aprovado.
# Quando o pagamento for recusado, deverá publicar um evento utilizando a routing
# key pagamento.recusado.
import json
import random

import pika

from helpers.helper import EXCHANGE_ECOMMERCE_NAME, init_ecommerce_exchange


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
            import ast

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
    resultado = processar_pagamento(pedido)

    ch.basic_publish(
        exchange=EXCHANGE_ECOMMERCE_NAME,
        routing_key=resultado["routing_key"],
        body=str(resultado["id"]),
    )

    print(f"[PAGAMENTO] Pedido {resultado['id']} -> {resultado['status']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    init_ecommerce_exchange(channel)

    queue_name = 'pagamento'
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(
        exchange=EXCHANGE_ECOMMERCE_NAME,
        queue=queue_name,
        routing_key='pedido.estoque_ok',
    )

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=receber_mensagem,
        auto_ack=False,
    )

    print("[PAGAMENTO] Aguardando eventos do RabbitMQ...")
    channel.start_consuming()