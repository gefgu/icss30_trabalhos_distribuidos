# O Microsserviço Promoções é responsável pela geração e publicação de promoções de
# produtos. O serviço deverá gerar promoções aleatórias de produtos e publicá-las no
# RabbitMQ, utilizando routing keys que indiquem a categoria do produto,
# como promocao.categoria.A, promocao.categoria.B, promocao.categoria.C