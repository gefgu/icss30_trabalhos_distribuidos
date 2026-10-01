# Consome pedido.estoque_ok.
# • (0,1) Solicita a criação de uma cobrança ao Mock de Pagamento para
# este gerar a URL de checkout, passando a sua URL de Webhook para
# retorno.
# • (0,1) Disponibiliza o endpoint HTTP para receber o Webhook do
# Mock de Pagamento com o status APROVADO ou RECUSADO.
# • Publica no RabbitMQ os
# eventos pagamento.aprovado ou pagamento.recusado.


