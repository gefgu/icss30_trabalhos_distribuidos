# Processos consumidores de promoções apenas receberão notificações sobre promoções
# de produtos. Executem dois processos consumidores: o consumidor C1 registrará
# interesse nas categorias de produtos A e B e o consumidor C2 registrará interesse em
# todas as categorias. Cada consumidor deve criar sua própria fila e associá-la às routing
# keys correspondentes às categorias de interesse. Atenção: esses processos não podem
# realizar chamadas para nenhum dos microsserviços. Eles devem se comunicar
# exclusivamente com o RabbitMQ, consumindo eventos de promoções.

import pika
from helpers.helper import init_promocoes_exchange, EXCHANGE_PROMOCOES_NAME
import os
import sys


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    init_promocoes_exchange(channel)

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

    def callback_c1(ch, method, properties, body):
        print(f"Consumidor C1 recebeu promoção: {body.decode()}")

    def callback_c2(ch, method, properties, body):
        print(f"Consumidor C2 recebeu promoção: {body.decode()}")

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
