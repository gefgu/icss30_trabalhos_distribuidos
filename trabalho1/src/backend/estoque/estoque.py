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

import pika


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()