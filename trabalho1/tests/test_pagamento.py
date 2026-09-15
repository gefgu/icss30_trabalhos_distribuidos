import os
import sys
import unittest
from unittest.mock import patch

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from backend.pagamento.pagamento import processar_pagamento  # noqa: E402


class TestPagamento(unittest.TestCase):
    def test_processar_pagamento_aprovado(self):
        with patch("backend.pagamento.pagamento.random.choice", return_value="aprovado"):
            resultado = processar_pagamento({"id": 9})

        self.assertEqual(resultado["status"], "aprovado")
        self.assertEqual(resultado["routing_key"], "pagamento.aprovado")

    def test_processar_pagamento_recusado(self):
        with patch("backend.pagamento.pagamento.random.choice", return_value="recusado"):
            resultado = processar_pagamento({"id": 10})

        self.assertEqual(resultado["status"], "recusado")
        self.assertEqual(resultado["routing_key"], "pagamento.recusado")


if __name__ == "__main__":
    unittest.main()
