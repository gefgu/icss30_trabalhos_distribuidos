# (0,5) Microsserviço Principal
# (0,2) Responsável por realizar a interação com os usuários por meio do terminal. Ele
# apresenta as opções do sistema e permite que os usuários executem ações como:
# • visualizar produtos;
# • realizar pedidos;
# excluir pedidos;
# • consultar seus pedidos e respectivos status.
# Cada novo pedido recebido deverá ser publicado como um evento no RabbitMQ,
# utilizando a routing key pedido.criado. Esse evento deverá conter, no mínimo, o
# identificador do pedido, os produtos, quantidades e informações necessárias para o
# processamento do pedido.
# (0,3) O microsserviço Principal deverá consumir os eventos:
# • pagamento.aprovado;
# • pagamento.recusado;
# • pedido.enviado;
# • pedido.estoque_ok;
# • estoque.indisponivel.
# A partir desses eventos, o microsserviço principal deverá atualizar o status dos
# respectivos pedidos.
# Quando um produto não estiver disponível em estoque ou quando o pagamento de um
# pedido for recusado, o microsserviço Principal deverá publicar um evento utilizando a
# routing key pedido.excluido.

import pika
import os
import sys
import subprocess
from time import sleep

from helpers.helper import EXCHANGE_ECOMMERCE_NAME

produtos = [
    {"id": 1, "nome": "Produto A", "categoria": "A", "estoque": 5},
    {"id": 2, "nome": "Produto B", "categoria": "B", "estoque": 3},
    {"id": 3, "nome": "Produto C", "categoria": "C", "estoque": 0},
    {"id": 4, "nome": "Produto A1", "categoria": "A", "estoque": 10},
]


class MenuInterativo:
    def __init__(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        self.channel = connection.channel()

        queue_pagamento_aprovado = "pagamento_aprovado"
        self.channel.queue_declare(
            queue=queue_pagamento_aprovado, durable=True, exclusive=False, auto_delete=False
        )
        self.channel.queue_bind(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            queue=queue_pagamento_aprovado,
            routing_key='pagamento.aprovado'
        )
        self.channel.basic_consume(
            queue=queue_pagamento_aprovado,
            on_message_callback=self.processa_pagemento_aprovado,
            auto_ack=True
        )


        queue_pagamento_recusado = "pagamento_recusado"
        self.channel.queue_declare(
            queue=queue_pagamento_recusado, durable=True, exclusive=False, auto_delete=False)
        self.channel.queue_bind(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            queue=queue_pagamento_recusado,
            routing_key='pagamento.recusado'
        )
        self.channel.basic_consume(
            queue=queue_pagamento_recusado,
            on_message_callback=self.processa_pagemento_recusado,
            auto_ack=True
        )


        queue_pedidos_enviados = "pedidos_enviados"
        self.channel.queue_declare(
            queue=queue_pedidos_enviados, durable=True, exclusive=False, auto_delete=False)
        self.channel.queue_bind(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            queue=queue_pedidos_enviados,
            routing_key='pedido.criado'
        )
        self.channel.basic_consume(
            queue=queue_pedidos_enviados,
            on_message_callback=self.processa_pedido_enviado,
            auto_ack=True
        )


        queue_pedidos_estoque_ok = "pedidos_estoque_ok"
        self.channel.queue_declare(
            queue=queue_pedidos_estoque_ok, durable=True, exclusive=False, auto_delete=False)
        self.channel.queue_bind(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            queue=queue_pedidos_estoque_ok,
            routing_key='pedido.estoque_ok'
        )
        self.channel.basic_consume(
            queue=queue_pedidos_estoque_ok,
            on_message_callback=self.processa_pedido_estoque_ok,
            auto_ack=True
        )

        queue_estoque_indisponivel = "estoque_indisponivel"
        self.channel.queue_declare(
            queue=queue_estoque_indisponivel, durable=True, exclusive=False, auto_delete=False)
        self.channel.queue_bind(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            queue=queue_estoque_indisponivel,
            routing_key='estoque.indisponivel'
        )
        self.channel.basic_consume(
            queue=queue_estoque_indisponivel,
            on_message_callback=self.processa_estoque_indisponivel,
            auto_ack=True
        )


        self.channel.start_consuming()


        

    def limpar_tela(self):
        subprocess.run("cls" if os.name == "nt" else "clear", shell=True)

    def exibir_menu(self):
        while True:
            self.limpar_tela()
            print("=== Menu Interativo ===")
            print("1. Visualizar produtos")
            print("2. Realizar pedido")
            print("3. Excluir pedido")
            print("4. Consultar pedidos")
            print("5. Sair")

            opcao = input("\nEscolha uma opção: ")
            
            if opcao == '1':
                self.visualizar_produtos()
            elif opcao == '2':
                self.realizar_pedidos()
            elif opcao == '3':
                self.excluir_pedidos()
            elif opcao == '4':
                self.consultar_pedidos()
            elif opcao == '5':
                print("\nSaindo do sistema. Até logo!\n")
                break
            else:
                input("\nOpção inválida! Pressione [ENTER] para tentar novamente.")


    def visualizar_produtos(self):
        print("\n=== Lista de Produtos ===")
        for produto in produtos:
            print(
                f"ID: {produto['id']}, Nome: {produto['nome']}, Categoria: {produto['categoria']}, Estoque: {produto['estoque']}"
            )

        input("\nPressione [ENTER] para voltar ao menu principal.")


        
        

    def realizar_pedidos(self):
        print("\n=== Realizar Pedido ===")
        print("TODO")
        sleep(2)  # Simula o tempo de carregamento

    def excluir_pedidos(self):
        print("\n=== Excluir Pedido ===")
        print("TODO")
        sleep(2)  # Simula o tempo de carregamento

    def consultar_pedidos(self):
        print("\n=== Consultar Pedidos ===")
        print("TODO")
        sleep(2)  # Simula o tempo de carregamento

    def processa_pagemento_aprovado(self, ch, method, properties, body):
        print(f"Pagamento aprovado: {body.decode()}")

    def processa_pagemento_recusado(self, ch, method, properties, body):
        print(f"Pagamento recusado: {body.decode()}")

    def processa_pedido_enviado(self, ch, method, properties, body):
        print(f"Pedido enviado: {body.decode()}")

    def processa_pedido_estoque_ok(self, ch, method, properties, body):
        print(f"Pedido estoque ok: {body.decode()}")

    def processa_estoque_indisponivel(self, ch, method, properties, body):
        print(f"Estoque indisponível: {body.decode()}")



def main():
    menu = MenuInterativo()
    menu.exibir_menu()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrompendo os consumidores...")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
