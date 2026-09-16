# Processos consumidores de promoções apenas receberão notificações sobre promoções
# de produtos. Executem dois processos consumidores: o consumidor C1 registrará
# interesse nas categorias de produtos A e B e o consumidor C2 registrará interesse em
# todas as categorias. Cada consumidor deve criar sua própria fila e associá-la às routing
# keys correspondentes às categorias de interesse. Atenção: esses processos não podem
# realizar chamadas para nenhum dos microsserviços. Eles devem se comunicar
# exclusivamente com o RabbitMQ, consumindo eventos de promoções.

import pika
from helpers.helper import (
    create_cryptography_keys,
    init_promocoes_exchange,
    EXCHANGE_PROMOCOES_NAME,
    verificar_assinatura,
)
import os
import sys
from pathlib import Path
from Crypto.PublicKey import RSA

FILE_FOLDER_PATH = Path(__file__).resolve().parents[0]

PRIVATE_KEY_FILE = FILE_FOLDER_PATH / "consumidores_promocoes_private.pem"
PUBLIC_KEY_FILE = FILE_FOLDER_PATH / "consumidores_promocoes_public.pem"
PROMOCOES_PUBLIC_KEY_FILE = (
    FILE_FOLDER_PATH.parent.parent / "promocoes" / "promocoes_public.pem"
)


def callback_consumidores(ch, method, properties, body):
    body_str = (
        body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
    )

    signature_in = None
    if properties.headers and "signature" in properties.headers:
        signature_in = properties.headers["signature"]

    if signature_in is None or not verificar_assinatura(
        body_str, signature_in, PROMOCOES_PUBLIC_KEY_FILE
    ):
        print("Assinatura inválida. Promoção descartada.")
        return

    print(f"Promoção {body_str} válida. Processando...")


def callback_c1(ch, method, properties, body):
    print(f"Consumidor C1 recebeu promoção")
    callback_consumidores(ch, method, properties, body)


def callback_c2(ch, method, properties, body):
    print(f"Consumidor C2 recebeu promoção")
    callback_consumidores(ch, method, properties, body)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    init_promocoes_exchange(channel)

    private_key = create_cryptography_keys(PRIVATE_KEY_FILE, PUBLIC_KEY_FILE)

    # Consumidor C1: Interesse nas categorias A e B
    queue_name_c1 = "consumidor_c1"
    channel.queue_declare(
        queue=queue_name_c1, durable=True, exclusive=False, auto_delete=False
    )
    channel.queue_bind(
        exchange=EXCHANGE_PROMOCOES_NAME,
        queue=queue_name_c1,
        routing_key="promocao.categoria.A",
    )
    channel.queue_bind(
        exchange=EXCHANGE_PROMOCOES_NAME,
        queue=queue_name_c1,
        routing_key="promocao.categoria.B",
    )

    # Consumidor C2: Interesse em todas as categorias
    queue_name_c2 = "consumidor_c2"
    channel.queue_declare(
        queue=queue_name_c2, durable=True, exclusive=False, auto_delete=False
    )
    channel.queue_bind(
        exchange=EXCHANGE_PROMOCOES_NAME,
        queue=queue_name_c2,
        routing_key="promocao.categoria.*",
    )

    print("Consumidores C1 e C2 estão aguardando promoções...")

    channel.basic_consume(
        queue=queue_name_c1, on_message_callback=callback_c1, auto_ack=True
    )
    channel.basic_consume(
        queue=queue_name_c2, on_message_callback=callback_c2, auto_ack=True
    )

    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrompendo os consumidores...")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
