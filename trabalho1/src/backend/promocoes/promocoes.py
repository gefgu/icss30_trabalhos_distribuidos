# O Microsserviço Promoções é responsável pela geração e publicação de promoções de
# produtos. O serviço deverá gerar promoções aleatórias de produtos e publicá-las no
# RabbitMQ, utilizando routing keys que indiquem a categoria do produto,
# como promocao.categoria.A, promocao.categoria.B, promocao.categoria.C

import json
import random
from time import sleep
import pika
from pathlib import Path


from helpers.helper import (
    EXCHANGE_PROMOCOES_NAME,
    assinar_mensagem,
    create_cryptography_keys,
    init_promocoes_exchange,
)

produtos = [
    {"id": 1, "nome": "Produto A", "categoria": "A"},
    {"id": 2, "nome": "Produto B", "categoria": "B"},
    {"id": 3, "nome": "Produto C", "categoria": "C"},
    {"id": 4, "nome": "Produto A1", "categoria": "A"},
]


FILE_FOLDER_PATH = Path(__file__).resolve().parents[0]

PRIVATE_KEY_FILE = FILE_FOLDER_PATH / "promocoes_private.pem"
PUBLIC_KEY_FILE = FILE_FOLDER_PATH / "promocoes_public.pem"


def gerar_promocao():
    """
    Gera uma promoção aleatória para um dos produtos disponíveis.
    """
    produto = random.choice(produtos)
    desconto = random.choice([10, 15, 20, 25, 30, 50])

    promocao = {
        "produto_id": produto["id"],
        "nome": produto["nome"],
        "categoria": produto["categoria"],
        "desconto": desconto,
    }
    routing_key = f"promocao.categoria.{produto['categoria']}"
    return promocao, routing_key


if __name__ == "__main__":
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    init_promocoes_exchange(channel)

    create_cryptography_keys(PRIVATE_KEY_FILE, PUBLIC_KEY_FILE)

    print("[PROMOCOES] Gerando e publicando promoções...")
    try:
        while True:
            promocao, routing_key = gerar_promocao()

            body_out = str(promocao)
            signature_out = assinar_mensagem(body_out, PRIVATE_KEY_FILE)

            channel.basic_publish(
                exchange=EXCHANGE_PROMOCOES_NAME,
                routing_key=routing_key,
                body=body_out.encode("utf-8"),
                properties=pika.BasicProperties(headers={"signature": signature_out}),
            )

            print(
                f"[PROMOCOES] {routing_key} -> {promocao['nome']} "
                f"com {promocao['desconto']}% de desconto"
            )

            sleep(5)
    except KeyboardInterrupt:
        print("\n[PROMOCOES] Encerrando geração de promoções.")
        connection.close()
