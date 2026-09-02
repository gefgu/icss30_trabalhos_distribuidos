# O Microsserviço Entrega é responsável pelo gerenciamento da emissão de notas e da
# entrega dos produtos.
# O serviço deverá consumir o evento pagamento.aprovado.
# Após receber esse evento, deverá realizar as operações necessárias para emissão da nota
# e preparação da entrega. Após o processamento, deverá publicar um novo evento
# utilizando a routing key pedido.enviado, informando que o pedido foi enviado.