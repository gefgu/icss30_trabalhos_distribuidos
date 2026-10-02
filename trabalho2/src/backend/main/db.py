import json
from pathlib import Path
import sqlite3

DB_PATH = Path(__file__).with_name("main.db")


def conectar():
    return sqlite3.connect(DB_PATH)


def inicializar_banco():
    with conectar() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS pedidos (
                id INTEGER PRIMARY KEY,
                produtos TEXT NOT NULL,
                estoque TEXT DEFAULT 'pendente',
                pagamento TEXT DEFAULT 'pendente',
                envio TEXT DEFAULT 'pendente'
            )
        """)


def criar_pedido(pedido):
    with conectar() as con:
        cursor = con.execute(
            """
            INSERT INTO pedidos (produtos, estoque, pagamento, envio)
            VALUES (?, ?, ?, ?)
        """,
            (
                json.dumps(pedido["produtos"]),
                pedido.get("estoque", "pendente"),
                pedido.get("pagamento", "pendente"),
                pedido.get("envio", "pendente"),
            ),
        )
        return cursor.lastrowid


def atualizar_pedido(id_pedido, campo, valor):
    with conectar() as con:
        con.execute(
            f"""
            UPDATE pedidos
            SET {campo} = ?
            WHERE id = ?
        """,
            (valor, id_pedido),
        )


def buscar_pedido(id_pedido):
    with conectar() as con:
        cursor = con.execute(
            """
            SELECT id, produtos, estoque, pagamento, envio
            FROM pedidos
            WHERE id = ?
        """,
            (id_pedido,),
        )
        row = cursor.fetchone()
        if row:
            return {
                "id": row[0],
                "produtos": json.loads(row[1]),
                "estoque": row[2],
                "pagamento": row[3],
                "envio": row[4],
            }
        return None
