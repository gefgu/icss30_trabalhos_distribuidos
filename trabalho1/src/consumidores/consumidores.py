# Processos consumidores de promoções apenas receberão notificações sobre promoções
# de produtos. Executem dois processos consumidores: o consumidor C1 registrará
# interesse nas categorias de produtos A e B e o consumidor C2 registrará interesse em
# todas as categorias. Cada consumidor deve criar sua própria fila e associá-la às routing
# keys correspondentes às categorias de interesse. Atenção: esses processos não podem
# realizar chamadas para nenhum dos microsserviços. Eles devem se comunicar
# exclusivamente com o RabbitMQ, consumindo eventos de promoções.


