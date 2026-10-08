# Consome eventos de Estoque, Pagamento e Entrega e persiste os status dos pedidos.

import ast
import json

import pika

from backend.main import db
from helpers.helper import EXCHANGE_ECOMMERCE_NAME, init_ecommerce_exchange

EVENTOS = {
    "pagamento.aprovado": ("pagamento", "aprovado"),
    "pagamento.recusado": ("pagamento", "recusado"),
    "pedido.enviado": ("envio", "enviado"),
    "pedido.estoque_ok": ("estoque", "ok"),
    "estoque.indisponivel": ("estoque", "indisponível"),
}


def processar_evento(ch, method, properties, body, callback=None):
    conteudo = body.decode("utf-8") if isinstance(body, bytes) else str(body)
    try:
        evento = json.loads(conteudo)
    except json.JSONDecodeError:
        evento = ast.literal_eval(conteudo)  # Compatibilidade com mensagens antigas.

    id_pedido = evento.get("id") if isinstance(evento, dict) else evento
    if method.routing_key == "pagamento.checkout_criado":
        checkout_url = evento.get("url") if isinstance(evento, dict) else None
        if callback and checkout_url:
            callback(id_pedido, {"url": checkout_url})
        return

    status = EVENTOS.get(method.routing_key)
    if status and db.buscar_pedido(id_pedido):
        campo, valor = status
        db.atualizar_pedido(id_pedido, campo, valor)
        print(f"Pedido {id_pedido}: {campo} = {valor}")

        if method.routing_key in {"pagamento.recusado", "estoque.indisponivel"}:
            ch.basic_publish(
                exchange=EXCHANGE_ECOMMERCE_NAME,
                routing_key="pedido.excluido",
                body=json.dumps({"id": id_pedido}),
            )
        if callback:
            callback(id_pedido, {"campo": campo, "status": valor})


def iniciar_consumo(callback=None):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    init_ecommerce_exchange(channel)

    filas = {
        "pagamento_checkout_criado": "pagamento.checkout_criado",
        "pagamento_aprovado": "pagamento.aprovado",
        "pagamento_recusado": "pagamento.recusado",
        "pedidos_enviados": "pedido.enviado",
        "pedidos_estoque_ok": "pedido.estoque_ok",
        "estoque_indisponivel": "estoque.indisponivel",
    }
    for fila, routing_key in filas.items():
        channel.queue_declare(queue=fila, durable=True)
        channel.queue_bind(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            queue=fila,
            routing_key=routing_key,
        )
        channel.basic_consume(
            queue=fila,
            on_message_callback=lambda ch, method, properties, body: processar_evento(
                ch, method, properties, body, callback
            ),
            auto_ack=True,
        )

    channel.start_consuming()
