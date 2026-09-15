# O Microsserviço Estoque é responsável pelo gerenciamento do estoque dos produtos.
# O serviço deverá consumir os eventos:
# • pedido.criado;
# • pedido.excluido.
# Ao receber um evento pedido.criado, o microsserviço Estoque deverá verificar a
# disponibilidade dos produtos solicitados.
# Caso todos os produtos estejam disponíveis, o serviço deverá realizar a respectiva
# reserva/baixa no estoque e publicar um evento utilizando a routing
# key pedido.estoque_ok, indicando que o pedido está apto a prosseguir para o
# processamento do pagamento.
# Caso algum produto não esteja disponível, o microsserviço Estoque deverá publicar um
# evento utilizando a routing key estoque.indisponivel, informando o pedido que não
# pode ser atendido.
# Ao receber um evento pedido.excluido, o microsserviço Estoque deverá devolver ao
# estoque os produtos que haviam sido reservados para o pedido.

import ast
import json
from Crypto.PublicKey import RSA
import pika
from pathlib import Path

from helpers.helper import (
    EXCHANGE_ECOMMERCE_NAME,
    init_ecommerce_exchange,
    assinar_mensagem,
    verificar_assinatura
)


produtos = [
    {"id": 1, "nome": "Produto A", "categoria": "A", "estoque": 5},
    {"id": 2, "nome": "Produto B", "categoria": "B", "estoque": 3},
    {"id": 3, "nome": "Produto C", "categoria": "C", "estoque": 0},
    {"id": 4, "nome": "Produto A1", "categoria": "A", "estoque": 10},
]

reservas = {}

FILE_FOLDER_PATH = Path(__file__).resolve().parents[0]

PRIVATE_KEY_FILE = FILE_FOLDER_PATH / "estoque_private.pem"
PUBLIC_KEY_FILE = FILE_FOLDER_PATH / "estoque_public.pem"


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
    if isinstance(dados, dict):
        pedido_id = dados.get("id")
    else:
        pedido_id = dados

    if isinstance(pedido_id, str):
        pedido_id = pedido_id.strip()
        if pedido_id.isdigit():
            return int(pedido_id)
        try:
            return int(ast.literal_eval(pedido_id))
        except (ValueError, SyntaxError):
            return pedido_id

    return pedido_id


def processar_pedido(pedido):
    dados = pedido if isinstance(pedido, dict) else _parse_mensagem(pedido)
    pedido_id = _obter_id_pedido(dados)
    itens = dados.get("produtos", []) if isinstance(dados, dict) else []

    if not itens:
        return {
            "id": pedido_id,
            "status": "indisponivel",
            "mensagem": "Pedido sem produtos.",
        }

    itens_para_reservar = []
    for item in itens:
        produto_id = item.get("id") if isinstance(item, dict) else item
        quantidade = item.get("quantidade", 1) if isinstance(item, dict) else 1
        produto = next((p for p in produtos if p["id"] == produto_id), None)

        if produto is None:
            return {
                "id": pedido_id,
                "status": "indisponivel",
                "mensagem": f"Produto {produto_id} não encontrado.",
            }

        if produto["estoque"] < quantidade:
            return {
                "id": pedido_id,
                "status": "indisponivel",
                "mensagem": f"Produto {produto_id} sem estoque suficiente.",
            }

        itens_para_reservar.append({"id": produto_id, "quantidade": quantidade})

    for item in itens_para_reservar:
        produto = next((p for p in produtos if p["id"] == item["id"]), None)
        if produto is not None:
            produto["estoque"] -= item["quantidade"]

    reservas[pedido_id] = itens_para_reservar
    return {
        "id": pedido_id,
        "status": "estoque_ok",
        "mensagem": "Produto(s) reservados com sucesso.",
    }


def processar_exclusao(pedido_id):
    id_pedido = _obter_id_pedido(pedido_id)
    itens_reservados = reservas.pop(id_pedido, [])

    for item in itens_reservados:
        produto = next((p for p in produtos if p["id"] == item["id"]), None)
        if produto is not None:
            produto["estoque"] += item["quantidade"]

    return {
        "id": id_pedido,
        "status": "pedido_cancelado",
        "mensagem": "Produtos devolvidos ao estoque.",
    }


def receber_mensagem(ch, method, properties, body):
    mensagem = _parse_mensagem(body)
    routing_key = method.routing_key

    if routing_key == "pedido.criado":
        # verify signature from producer (main) using its public key
        body_str = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
        signature_in = None
        if properties and getattr(properties, "headers", None):
            signature_in = properties.headers.get("signature")

        producer_pub = FILE_FOLDER_PATH.parent / "main" / "principal_public.pem"
        if signature_in is None or not verificar_assinatura(body_str, signature_in, producer_pub):
            print(f"[ESTOQUE] Assinatura inválida no pedido.criado: {mensagem}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        resultado = processar_pedido(mensagem)
        pedido_id = resultado["id"]

        # publish as plain string and put signature in header
        payload = {"id": pedido_id}
        body_out = str(payload)
        signature_out = assinar_mensagem(body_out, PRIVATE_KEY_FILE)

        if resultado["status"] == "estoque_ok":
            ch.basic_publish(
                exchange=EXCHANGE_ECOMMERCE_NAME,
                routing_key="pedido.estoque_ok",
                body=body_out,
                properties=pika.BasicProperties(headers={"signature": signature_out}),
            )
            print(f"[ESTOQUE] Pedido {pedido_id} passou pela validação do estoque.")
        else:
            ch.basic_publish(
                exchange=EXCHANGE_ECOMMERCE_NAME,
                routing_key="estoque.indisponivel",
                body=body_out,
                properties=pika.BasicProperties(headers={"signature": signature_out}),
            )
            print(f"[ESTOQUE] Pedido {pedido_id} indisponível: {resultado['mensagem']}")

    elif routing_key == "pedido.excluido":
        body_str = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
        signature_in = None
        if properties and getattr(properties, "headers", None):
            signature_in = properties.headers.get("signature")

        producer_pub = FILE_FOLDER_PATH.parent / "main" / "principal_public.pem"
        if signature_in is None or not verificar_assinatura(body_str, signature_in, producer_pub):
            print(f"[ESTOQUE] Assinatura inválida no pedido.excluido: {mensagem}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        pedido_id = _obter_id_pedido(mensagem)
        resultado = processar_exclusao(pedido_id)
        print(f"[ESTOQUE] Pedido {resultado['id']} cancelado e devolvido ao estoque.")

    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    init_ecommerce_exchange(channel)

    key = RSA.generate(2048)
    private_key = key.export_key()
    with open(PRIVATE_KEY_FILE, "wb") as f:
        f.write(private_key)

    public_key = key.publickey().export_key()
    with open(PUBLIC_KEY_FILE, "wb") as f:
        f.write(public_key)

    queue_name = "estoque"
    channel.queue_declare(queue=queue_name, durable=True)

    channel.queue_bind(
        exchange=EXCHANGE_ECOMMERCE_NAME,
        queue=queue_name,
        routing_key="pedido.criado",
    )

    channel.queue_bind(
        exchange=EXCHANGE_ECOMMERCE_NAME,
        queue=queue_name,
        routing_key="pedido.excluido",
    )

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=receber_mensagem,
        auto_ack=False,
    )

    print("[ESTOQUE] Aguardando eventos do RabbitMQ...")
    channel.start_consuming()
