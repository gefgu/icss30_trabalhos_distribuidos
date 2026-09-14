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

pedidos = [
    {
        "id": 1,
        "produtos": [{"id": 1, "quantidade": 2}, {"id": 2, "quantidade": 1}],
        "estoque": None,
        "pagamento": None,
    },
]


class MenuInterativo:
    def __init__(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )
        self.channel = connection.channel()

        queue_pagamento_aprovado = "pagamento_aprovado"
        self.channel.queue_declare(
            queue=queue_pagamento_aprovado,
            durable=True,
            exclusive=False,
            auto_delete=False,
        )
        self.channel.queue_bind(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            queue=queue_pagamento_aprovado,
            routing_key="pagamento.aprovado",
        )
        self.channel.basic_consume(
            queue=queue_pagamento_aprovado,
            on_message_callback=self.processa_pagamento_aprovado,
            auto_ack=True,
        )

        queue_pagamento_recusado = "pagamento_recusado"
        self.channel.queue_declare(
            queue=queue_pagamento_recusado,
            durable=True,
            exclusive=False,
            auto_delete=False,
        )
        self.channel.queue_bind(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            queue=queue_pagamento_recusado,
            routing_key="pagamento.recusado",
        )
        self.channel.basic_consume(
            queue=queue_pagamento_recusado,
            on_message_callback=self.processa_pagamento_recusado,
            auto_ack=True,
        )

        queue_pedidos_enviados = "pedidos_enviados"
        self.channel.queue_declare(
            queue=queue_pedidos_enviados,
            durable=True,
            exclusive=False,
            auto_delete=False,
        )
        self.channel.queue_bind(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            queue=queue_pedidos_enviados,
            routing_key="pedido.criado",
        )
        self.channel.basic_consume(
            queue=queue_pedidos_enviados,
            on_message_callback=self.processa_pedido_enviado,
            auto_ack=True,
        )

        queue_pedidos_estoque_ok = "pedidos_estoque_ok"
        self.channel.queue_declare(
            queue=queue_pedidos_estoque_ok,
            durable=True,
            exclusive=False,
            auto_delete=False,
        )
        self.channel.queue_bind(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            queue=queue_pedidos_estoque_ok,
            routing_key="pedido.estoque_ok",
        )
        self.channel.basic_consume(
            queue=queue_pedidos_estoque_ok,
            on_message_callback=self.processa_pedido_estoque_ok,
            auto_ack=True,
        )

        queue_estoque_indisponivel = "estoque_indisponivel"
        self.channel.queue_declare(
            queue=queue_estoque_indisponivel,
            durable=True,
            exclusive=False,
            auto_delete=False,
        )
        self.channel.queue_bind(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            queue=queue_estoque_indisponivel,
            routing_key="estoque.indisponivel",
        )
        self.channel.basic_consume(
            queue=queue_estoque_indisponivel,
            on_message_callback=self.processa_estoque_indisponivel,
            auto_ack=True,
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

            if opcao == "1":
                self.visualizar_produtos()
            elif opcao == "2":
                self.realizar_pedidos()
            elif opcao == "3":
                self.excluir_pedidos()
            elif opcao == "4":
                self.consultar_pedidos()
            elif opcao == "5":
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
        print(
            "\nDigite os IDs dos produtos que deseja solicitar (separados por vírgula):"
        )
        ids_produtos = input().split(",")
        produtos_selecionados = []
        for id_produto in ids_produtos:
            produto = next(
                (p for p in produtos if str(p["id"]) == id_produto.strip()), None
            )
            if produto:
                quantidade = int(
                    input(f"Digite a quantidade para o produto {produto['nome']}: ")
                )
                produtos_selecionados.append(
                    {"id": produto["id"], "quantidade": quantidade}
                )
            else:
                print(f"Produto com ID {id_produto.strip()} não encontrado.")

        if produtos_selecionados:
            print("\nProdutos selecionados:")
            for produto in produtos_selecionados:
                produto_info = next(
                    (p for p in produtos if p["id"] == produto["id"]), None
                )
                if produto_info:
                    print(
                        f"- {produto_info['nome']} (Quantidade: {produto['quantidade']})"
                    )

        # Cria um novo pedido com ID incremental
        novo_id_pedido = max([p["id"] for p in pedidos], default=0) + 1
        novo_pedido = {
            "id": novo_id_pedido,
            "produtos": produtos_selecionados,
            "estoque": None,
            "pagamento": None,
        }

        self.channel.basic_publish(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            routing_key="pedido.criado",
            body=str(novo_pedido),
        )

        pedidos.append(novo_pedido)

    def excluir_pedidos(self):
        print("\n=== Excluir Pedido ===")
        print("\nDigite o ID do pedido que deseja excluir:")
        id_pedido = input()

        # Encontra o pedido com o ID informado
        pedido = next((p for p in pedidos if p["id"] == id_pedido), None)
        if pedido:
            pedidos.remove(pedido)
            # Publica o evento de exclusão do pedido no RabbitMQ
            self.channel.basic_publish(
                exchange=EXCHANGE_ECOMMERCE_NAME,
                routing_key="pedido.excluido",
                body=str(id_pedido),
            )

            print(f"Pedido {id_pedido} excluído com sucesso.")
        else:
            print(f"Pedido {id_pedido} não encontrado.")

        input("\nPressione [ENTER] para voltar ao menu principal.")

    def consultar_pedidos(self):
        print("\n=== Consultar Pedidos ===")
        for pedido in pedidos:
            print(f"ID do Pedido: {pedido['id']}")
            print("Produtos:")
            for produto in pedido["produtos"]:
                produto_info = next(
                    (p for p in produtos if p["id"] == produto["id"]), None
                )
                if produto_info:
                    print(
                        f"- {produto_info['nome']} (Quantidade: {produto['quantidade']})"
                    )
            print(f"Status do Estoque: {pedido['estoque']}")
            print(f"Status do Pagamento: {pedido['pagamento']}")
            print("------------------------")

        input("\nPressione [ENTER] para voltar ao menu principal.")

    def processa_pagamento_aprovado(self, ch, method, properties, body):
        print(f"\nPagamento aprovado: {body.decode()}")

        # Atualiza o status do pagamento do pedido
        id_pedido = body.decode()
        pedido = next((p for p in pedidos if p["id"] == id_pedido), None)
        if pedido:
            pedido["pagamento"] = "aprovado"

    def processa_pagamento_recusado(self, ch, method, properties, body):
        print(f"\nPagamento recusado: {body.decode()}")

        # Atualiza o status do pagamento do pedido
        id_pedido = body.decode()
        pedido = next((p for p in pedidos if p["id"] == id_pedido), None)
        if pedido:
            pedido["pagamento"] = "recusado"
            # Publica o evento de exclusão do pedido no RabbitMQ
            self.channel.basic_publish(
                exchange=EXCHANGE_ECOMMERCE_NAME,
                routing_key="pedido.excluido",
                body=str(id_pedido),
            )

    def processa_pedido_enviado(self, ch, method, properties, body):
        print(f"\nPedido enviado: {body.decode()}")

        # Atualiza o status do pedido
        id_pedido = body.decode()
        pedido = next((p for p in pedidos if p["id"] == id_pedido), None)
        if pedido:
            pedido["status"] = "enviado"

    def processa_pedido_estoque_ok(self, ch, method, properties, body):
        print(f"\nPedido estoque ok: {body.decode()}")

        # Atualiza o status do estoque do pedido
        id_pedido = body.decode()
        pedido = next((p for p in pedidos if p["id"] == id_pedido), None)
        if pedido:
            pedido["estoque"] = "ok"

    def processa_estoque_indisponivel(self, ch, method, properties, body):
        print(f"Estoque indisponível: {body.decode()}")

        id_pedido = body.decode()
        pedido = next((p for p in pedidos if p["id"] == id_pedido), None)
        if pedido:
            pedido["estoque"] = "indisponível"
            # Publica o evento de exclusão do pedido no RabbitMQ
            self.channel.basic_publish(
                exchange=EXCHANGE_ECOMMERCE_NAME,
                routing_key="pedido.excluido",
                body=str(id_pedido),
            )


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
