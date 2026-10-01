# Consumir todos os eventos gerados pelo microsserviços Estoque,
# Pagamento e Entrega (pedido.estoque_ok, estoque.indisponivel,
# pagamento.aprovado, pagamento.recusado, pedido.enviado)

import ast
import json
import os
import sys
import threading
import pika

from fastapi import FastAPI

from backend.main.consumer import iniciar_consumo
from helpers.helper import EXCHANGE_ECOMMERCE_NAME, EXCHANGE_ECOMMERCE_NAME, init_ecommerce_exchange


pedidos = []


def _body_as_string(body):
    return body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)

    
def _event_signature(properties):
    if properties and getattr(properties, "headers", None):
        return properties.headers.get("signature")
    return None


def _event_data(body):
    body_str = _body_as_string(body)
    try:
        return ast.literal_eval(body_str)
    except (ValueError, SyntaxError):
        return json.loads(body_str)


def _event_id(body):
    data = _event_data(body)
    return data.get("id") if isinstance(data, dict) else data


def normalizar_pedido_id(pedido_id):
    if isinstance(pedido_id, str):
        pedido_id = pedido_id.strip()
        if pedido_id.isdigit():
            return int(pedido_id)
        return pedido_id
    return int(pedido_id) if isinstance(pedido_id, (int, float)) and not isinstance(pedido_id, bool) else pedido_id


def _find_pedido(pedido_id):
    pedido_id = normalizar_pedido_id(pedido_id)
    return next((p for p in pedidos if p["id"] == pedido_id), None)

def processa_pagamento_aprovado(self, ch, method, properties, body):
    body_str = _body_as_string(body)

    id_pedido = _event_id(body)
    print(f"\nPagamento aprovado: {id_pedido}")
    pedido = _find_pedido(id_pedido)
    if pedido:
        pedido["pagamento"] = "aprovado"

def processa_pagamento_recusado(self, ch, method, properties, body):
    body_str = _body_as_string(body)
    signature = _event_signature(properties)

    id_pedido = _event_id(body)
    print(f"\nPagamento recusado: {id_pedido}")
    pedido = _find_pedido(id_pedido)
    if pedido:
        pedido["pagamento"] = "recusado"
        payload = {"id": id_pedido}
        body_out = str(payload)
        self.consumer_channel.basic_publish(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            routing_key="pedido.excluido",
            body=body_out,
        )

def processa_pedido_enviado(self, ch, method, properties, body):
    id_pedido = _event_id(body)
    print(f"\nPedido enviado: {id_pedido}")
    pedido = _find_pedido(id_pedido)
    if pedido:
        pedido["envio"] = "enviado"

def processa_pedido_estoque_ok(self, ch, method, properties, body):
    body_str = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)

    try:
        body_dict = ast.literal_eval(body_str)
    except Exception:
        body_dict = json.loads(body_str)

    id_pedido = body_dict.get("id")

    pedido = next((p for p in pedidos if p["id"] == id_pedido), None)
    if pedido:
        pedido["estoque"] = "ok"

def processa_estoque_indisponivel(self, ch, method, properties, body):
    body_str = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)

    try:
        body_dict = ast.literal_eval(body_str)
    except Exception:
        body_dict = json.loads(body_str)

    id_pedido = body_dict.get("id")
    pedido = next((p for p in pedidos if p["id"] == id_pedido), None)
    if pedido:
        pedido["estoque"] = "indisponível"
        payload = {"id": id_pedido}
        body_out = str(payload)
        self.consumer_channel.basic_publish(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            routing_key="pedido.excluido",
            body=body_out,
        )


def iniciar_consumo():
    connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )
    consumer_channel = connection.channel()
    init_ecommerce_exchange(consumer_channel)

    queue_pagamento_aprovado = "pagamento_aprovado"
    consumer_channel.queue_declare(
        queue=queue_pagamento_aprovado,
        durable=True,
        exclusive=False,
        auto_delete=False,
    )
    consumer_channel.queue_bind(
        exchange=EXCHANGE_ECOMMERCE_NAME,
        queue=queue_pagamento_aprovado,
        routing_key="pagamento.aprovado",
    )
    consumer_channel.basic_consume(
        queue=queue_pagamento_aprovado,
        on_message_callback=processa_pagamento_aprovado,
        auto_ack=True,
    )

    queue_pagamento_recusado = "pagamento_recusado"
    consumer_channel.queue_declare(
        queue=queue_pagamento_recusado,
        durable=True,
        exclusive=False,
        auto_delete=False,
    )
    consumer_channel.queue_bind(
        exchange=EXCHANGE_ECOMMERCE_NAME,
        queue=queue_pagamento_recusado,
        routing_key="pagamento.recusado",
    )
    consumer_channel.basic_consume(
        queue=queue_pagamento_recusado,
        on_message_callback=processa_pagamento_recusado,
        auto_ack=True,
    )

    queue_pedidos_enviados = "pedidos_enviados"
    consumer_channel.queue_declare(
        queue=queue_pedidos_enviados,
        durable=True,
        exclusive=False,
        auto_delete=False,
    )
    consumer_channel.queue_bind(
        exchange=EXCHANGE_ECOMMERCE_NAME,
        queue=queue_pedidos_enviados,
        routing_key="pedido.enviado",
    )
    consumer_channel.basic_consume(
        queue=queue_pedidos_enviados,
        on_message_callback=processa_pedido_enviado,
        auto_ack=True,
    )

    queue_pedidos_estoque_ok = "pedidos_estoque_ok"
    consumer_channel.queue_declare(
        queue=queue_pedidos_estoque_ok,
        durable=True,
        exclusive=False,
        auto_delete=False,
    )
    consumer_channel.queue_bind(
        exchange=EXCHANGE_ECOMMERCE_NAME,
        queue=queue_pedidos_estoque_ok,
        routing_key="pedido.estoque_ok",
    )
    consumer_channel.basic_consume(
        queue=queue_pedidos_estoque_ok,
        on_message_callback=processa_pedido_estoque_ok,
        auto_ack=True,
    )

    queue_estoque_indisponivel = "estoque_indisponivel"
    consumer_channel.queue_declare(
        queue=queue_estoque_indisponivel,
        durable=True,
        exclusive=False,
        auto_delete=False,
    )
    consumer_channel.queue_bind(
        exchange=EXCHANGE_ECOMMERCE_NAME,
        queue=queue_estoque_indisponivel,
        routing_key="estoque.indisponivel",
    )
    consumer_channel.basic_consume(
        queue=queue_estoque_indisponivel,
        on_message_callback=processa_estoque_indisponivel,
        auto_ack=True,
    )

    consumer_channel.start_consuming()