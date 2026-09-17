import pika

from backend.consumidores.helper import processar_promocao
from helpers.helper import EXCHANGE_PROMOCOES_NAME, init_promocoes_exchange


QUEUE_NAME = "consumidor_c2"


def callback(ch, method, properties, body):
    processar_promocao("C2", properties, body)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    init_promocoes_exchange(channel)

    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(
        exchange=EXCHANGE_PROMOCOES_NAME,
        queue=QUEUE_NAME,
        routing_key="promocao.categoria.*",
    )

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
    print("[C2] Aguardando promoções de todas as categorias...")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[C2] Consumidor interrompido.")
