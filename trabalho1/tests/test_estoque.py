import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from backend.estoque.estoque import (  # noqa: E402
    produtos,
    processar_pedido,
    processar_exclusao,
    reservas,
)


class TestEstoque(unittest.TestCase):
    def setUp(self):
        self.produtos_orig = [produto.copy() for produto in produtos]
        self.reservas_orig = dict(reservas)

    def tearDown(self):
        produtos[:] = self.produtos_orig
        reservas.clear()
        reservas.update(self.reservas_orig)

    def test_processar_pedido_com_estoque_disponivel(self):
        resultado = processar_pedido({"id": 99, "produtos": [{"id": 1, "quantidade": 2}]})
        self.assertEqual(resultado["status"], "estoque_ok")
        self.assertEqual(produtos[0]["estoque"], 3)
        self.assertIn(99, reservas)

    def test_processar_pedido_sem_estoque_disponivel(self):
        resultado = processar_pedido({"id": 100, "produtos": [{"id": 3, "quantidade": 1}]})
        self.assertEqual(resultado["status"], "indisponivel")
        self.assertEqual(produtos[2]["estoque"], 0)

    def test_processar_exclusao_retorna_produtos(self):
        processar_pedido({"id": 101, "produtos": [{"id": 2, "quantidade": 1}]})
        resultado = processar_exclusao(101)
        self.assertEqual(resultado["status"], "pedido_cancelado")
        self.assertEqual(produtos[1]["estoque"], 3)
        self.assertNotIn(101, reservas)


if __name__ == "__main__":
    unittest.main()
