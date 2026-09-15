# O Microsserviço Entrega é responsável pelo gerenciamento da emissão de notas e da
# entrega dos produtos.
# O serviço deverá consumir o evento pagamento.aprovado.
# Após receber esse evento, deverá realizar as operações necessárias para emissão da nota
# e preparação da entrega. Após o processamento, deverá publicar um novo evento
# utilizando a routing key pedido.enviado, informando que o pedido foi enviado.

import ast
import json

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
            return ast.literal_eval(conteudo)
        except (ValueError, SyntaxError):
            return {"id": conteudo}


def _obter_id_pedido(dados):
    pedido_id = dados.get("id") if isinstance(dados, dict) else dados

    if isinstance(pedido_id, str):
        pedido_id = pedido_id.strip()
        if pedido_id.isdigit():
            return int(pedido_id)

    return pedido_id


def emitir_nota(pedido_id):
    """
    Simula a emissão da nota fiscal do pedido.
    """
    print(f"[ENTREGA] Nota fiscal emitida para o pedido {pedido_id}.")


def preparar_entrega(pedido_id):
    """
    Simula a preparação da entrega do pedido.
    """
    print(f"[ENTREGA] Entrega do pedido {pedido_id} preparada.")


def processar_entrega(pedido):
    dados = pedido if isinstance(pedido, dict) else _parse_mensagem(pedido)
    pedido_id = _obter_id_pedido(dados)

    emitir_nota(pedido_id)
    preparar_entrega(pedido_id)

    return {"id": pedido_id, "status": "enviado", "routing_key": "pedido.enviado"}


def receber_mensagem(ch, method, properties, body):
    pedido = _parse_mensagem(body)
    resultado = processar_entrega(pedido)

    ch.basic_publish(
        exchange=EXCHANGE_ECOMMERCE_NAME,
        routing_key=resultado["routing_key"],
        body=str(resultado["id"]),
    )

    print(f"[ENTREGA] Pedido {resultado['id']} -> {resultado['status']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    init_ecommerce_exchange(channel)

    queue_name = 'entrega'
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(
        exchange=EXCHANGE_ECOMMERCE_NAME,
        queue=queue_name,
        routing_key='pagamento.aprovado',
    )

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=receber_mensagem,
        auto_ack=False,
    )

    print("[ENTREGA] Aguardando eventos do RabbitMQ...")
    channel.start_consuming()
