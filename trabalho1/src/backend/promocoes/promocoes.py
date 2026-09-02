# O Microsserviço Promoções é responsável pela geração e publicação de promoções de
# produtos. O serviço deverá gerar promoções aleatórias de produtos e publicá-las no
# RabbitMQ, utilizando routing keys que indiquem a categoria do produto,
# como promocao.categoria.A, promocao.categoria.B, promocao.categoria.C

import pika


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()