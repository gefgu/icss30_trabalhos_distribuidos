import json
import os
from pathlib import Path

PASTA_DADOS = Path(__file__).resolve().parents[3] / "dados"


# =============================================================
# Persistência em disco
# Três arquivos por nó, na pasta dados/no_<id>/:
#   estado.json -> termo atual e voto concedido (voted_for)
#   log.json    -> log de operações (termo, índice, dado)
#   banco.json  -> banco final: só dados comitados + commit_index
# =============================================================
class Persistencia:
    def __init__(self, id_no):
        self.pasta = PASTA_DADOS / f"no_{id_no}"
        self.pasta.mkdir(parents=True, exist_ok=True)

    def _ler(self, nome, padrao):
        arquivo = self.pasta / nome
        if not arquivo.exists():
            return padrao
        with open(arquivo, encoding="utf-8") as f:
            return json.load(f)

    def _gravar(self, nome, conteudo):
        # Escrita atômica: grava num .tmp e troca. Se o processo cair no meio,
        # o arquivo antigo continua inteiro.
        arquivo = self.pasta / nome
        temporario = arquivo.with_suffix(".tmp")
        with open(temporario, "w", encoding="utf-8") as f:
            json.dump(conteudo, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        temporario.replace(arquivo)

    def carregar(self):
        estado = self._ler("estado.json", {"termo": 0, "voted_for": None})
        entradas = self._ler("log.json", [])
        banco = self._ler("banco.json", {"commit_index": 0, "dados": []})
        return estado, entradas, banco

    def salvar_estado(self, termo, voted_for):
        self._gravar("estado.json", {"termo": termo, "voted_for": voted_for})

    def salvar_log(self, entradas):
        self._gravar("log.json", entradas)

    def salvar_banco(self, commit_index, dados):
        self._gravar("banco.json", {"commit_index": commit_index, "dados": dados})
