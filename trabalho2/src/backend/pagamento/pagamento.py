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
import random
import ast
import json
import pika
from pathlib import Path

from helpers.helper import (
    EXCHANGE_ECOMMERCE_NAME,
    init_ecommerce_exchange,
    assinar_mensagem,
    verificar_assinatura,
    create_cryptography_keys,
)

FILE_FOLDER_PATH = Path(__file__).resolve().parents[0]

PRIVATE_KEY_FILE = FILE_FOLDER_PATH / "pagamento_private.pem"
PUBLIC_KEY_FILE = FILE_FOLDER_PATH / "pagamento_public.pem"
PRINCIPAL_PUBLIC_KEY_FILE = FILE_FOLDER_PATH.parent / "main" / "principal_public.pem"
ESTOQUE_PUBLIC_KEY_FILE = FILE_FOLDER_PATH.parent / "estoque" / "estoque_public.pem"
ENTREGA_PUBLIC_KEY_FILE = FILE_FOLDER_PATH.parent / "entrega" / "entrega_public.pem"

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
        # verify signature from producer (estoque) using the stock public key
        body_str = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
        signature_in = None
        if properties and getattr(properties, "headers", None):
            signature_in = properties.headers.get("signature")

        if signature_in is None or not verificar_assinatura(body_str, signature_in, ESTOQUE_PUBLIC_KEY_FILE):
            print(f"[ESTOQUE] Assinatura inválida no pedido.estoque_ok: {pedido}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        resultado = processar_pagamento(pedido)
        pedido_id = resultado["id"]

        # publish as plain string and put signature in header
        payload = {"id": pedido_id}
        body_out = str(payload)
        signature_out = assinar_mensagem(body_out, PRIVATE_KEY_FILE)

        if resultado["status"] == "aprovado":
            ch.basic_publish(
                exchange=EXCHANGE_ECOMMERCE_NAME,
                routing_key="pagamento.aprovado",
                body=body_out,
                properties=pika.BasicProperties(headers={"signature": signature_out}),
            )
            print(f"[PAGAMENTO] Pedido {pedido_id} -> {resultado['status']}")
        else:
            ch.basic_publish(
                exchange=EXCHANGE_ECOMMERCE_NAME,
                routing_key="pagamento.recusado",
                body=body_out,
                properties=pika.BasicProperties(headers={"signature": signature_out}),
            )
            print(f"[PAGAMENTO] Pedido {pedido_id} -> {resultado['status']}")

        ch.basic_ack(delivery_tag=method.delivery_tag)
    


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    init_ecommerce_exchange(channel)

    create_cryptography_keys(PRIVATE_KEY_FILE, PUBLIC_KEY_FILE)

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
