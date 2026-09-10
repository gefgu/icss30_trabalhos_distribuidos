EXCHANGE_ECOMMERCE_NAME = 'ecommerce'
EXCHANGE_PROMOCOES_NAME = 'promocoes'


def init_promocoes_exchange(channel):
    """
    Inicializa o exchange de promoções no RabbitMQ.
    """
    channel.exchange_declare(exchange='promocoes', exchange_type='topic')

def init_ecommerce_exchange(channel):
    """
    Inicializa o exchange de e-commerce no RabbitMQ.
    """
    channel.exchange_declare(exchange='ecommerce', exchange_type='direct')