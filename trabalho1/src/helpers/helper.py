
EXCHANGE_PROMOCOES_NAME = 'promocoes'


def init_promocoes_exchange(channel):
    """
    Inicializa o exchange de promoções no RabbitMQ.
    """
    channel.exchange_declare(exchange='promocoes', exchange_type='topic')