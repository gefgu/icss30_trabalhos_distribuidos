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

class MenuInterativo:
    def __init__(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        self.channel = connection.channel()

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
        print("TODO")
        sleep(2)  # Simula o tempo de carregamento

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
