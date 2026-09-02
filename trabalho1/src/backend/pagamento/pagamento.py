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
import pika


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()