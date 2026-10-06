# MS Promoções (valor 0,4)
# • (0,1) Consome o evento interesse.promocao (e interesse.cancelado).
# • (0,3) Gera promoções e notifica por e-mail (API do Resend) os
#   consumidores cadastrados interessados na categoria.
#
# Como rodar (a partir de src/):
#   export RESEND_API_KEY="re_xxxxxxxx"
#   uv run python -m backend.promocoes.promocoes

import ast
import json
import os
import sqlite3
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pika

from helpers.helper import EXCHANGE_ECOMMERCE_NAME, init_ecommerce_exchange

# ---------------------------------------------------------------- Config
DB_PATH = Path(__file__).with_name("promocoes.db")
QUEUE_NAME = "promocoes"

RESEND_URL = "https://api.resend.com/emails"
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
# Sem domínio verificado, o Resend só aceita este remetente de teste
# e só entrega para o e-mail da própria conta Resend.
RESEND_FROM = os.environ.get("RESEND_FROM", "Loja <onboarding@resend.dev>")

CATEGORIAS = ["A", "B", "C"]


# ---------------------------------------------------------------- Banco
def conectar():
    return sqlite3.connect(DB_PATH)


def inicializar_banco():
    with conectar() as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS interesses (
                email     TEXT NOT NULL,
                categoria TEXT NOT NULL,
                PRIMARY KEY (email, categoria)
            )
            """
        )


def salvar_interesse(email, categoria):
    with conectar() as con:
        # OR IGNORE: se a pessoa se inscrever duas vezes, não duplica.
        con.execute(
            "INSERT OR IGNORE INTO interesses (email, categoria) VALUES (?, ?)",
            (email, categoria),
        )


def remover_interesse(email, categoria):
    with conectar() as con:
        if categoria == "*":
            # Cancelar "*" = sair de todas as categorias.
            con.execute("DELETE FROM interesses WHERE email = ?", (email,))
        else:
            con.execute(
                "DELETE FROM interesses WHERE email = ? AND categoria = ?",
                (email, categoria),
            )


def buscar_interessados(categoria):
    """E-mails inscritos na categoria OU em todas ('*')."""
    with conectar() as con:
        rows = con.execute(
            "SELECT DISTINCT email FROM interesses WHERE categoria IN (?, '*')",
            (categoria,),
        ).fetchall()
    return [r[0] for r in rows]


def listar_interesses():
    with conectar() as con:
        return con.execute(
            "SELECT email, categoria FROM interesses ORDER BY email"
        ).fetchall()


# ---------------------------------------------------------------- RabbitMQ
def _parse_mensagem(body):
    conteudo = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
    try:
        return json.loads(conteudo)
    except json.JSONDecodeError:
        # O Gateway ainda publica str(dict); isso mantém compatibilidade.
        return ast.literal_eval(conteudo)


def receber_mensagem(ch, method, properties, body):
    # TODO: verificar a assinatura digital do Gateway aqui quando o
    # helper com sign/verify voltar.
    try:
        dados = _parse_mensagem(body)
        email = dados["email"].strip().lower()
        categoria = dados["categoria"].strip().upper()

        if method.routing_key == "interesse.promocao":
            salvar_interesse(email, categoria)
            print(f"\n[PROMOCOES] {email} inscrito na categoria {categoria}")
        elif method.routing_key == "interesse.cancelado":
            remover_interesse(email, categoria)
            print(f"\n[PROMOCOES] {email} cancelou a categoria {categoria}")
    except (KeyError, ValueError, SyntaxError, AttributeError) as erro:
        print(f"\n[PROMOCOES] Mensagem inválida descartada: {erro}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def iniciar_consumo():
    # Conexão própria: o pika não pode ser compartilhado entre threads.
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    init_ecommerce_exchange(channel)

    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    for routing_key in ("interesse.promocao", "interesse.cancelado"):
        channel.queue_bind(
            exchange=EXCHANGE_ECOMMERCE_NAME,
            queue=QUEUE_NAME,
            routing_key=routing_key,
        )

    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=receber_mensagem,
        auto_ack=False,
    )
    channel.start_consuming()


# ---------------------------------------------------------------- E-mail
def enviar_email(destinatario, assunto, html):
    """Chama a API REST do Resend. Retorna True se deu certo."""
    payload = json.dumps(
        {"from": RESEND_FROM, "to": [destinatario], "subject": assunto, "html": html}
    ).encode("utf-8")

    req = urllib.request.Request(
        RESEND_URL,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
            # O Resend bloqueia o User-Agent padrão do urllib.
            "User-Agent": "ecommerce-promocoes/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resposta = json.loads(resp.read())
            print(f"[PROMOCOES] E-mail enviado para {destinatario} (id {resposta.get('id')})")
            return True
    except urllib.error.HTTPError as erro:
        print(f"[PROMOCOES] Falha ao enviar para {destinatario}: {erro.code} {erro.read().decode()}")
    except urllib.error.URLError as erro:
        print(f"[PROMOCOES] Sem conexão com o Resend: {erro.reason}")
    return False


def montar_html(categoria, descricao, desconto):
    return f"""
    <div style="font-family: sans-serif; max-width: 480px">
      <h2>🔥 Promoção na categoria {categoria}!</h2>
      <p>{descricao}</p>
      <p style="font-size: 24px"><strong>{desconto}% OFF</strong></p>
      <p style="color: #888; font-size: 12px">
        Você recebeu este e-mail porque se inscreveu nas promoções da categoria {categoria}.
      </p>
    </div>
    """


def gerar_promocao(categoria, descricao, desconto):
    interessados = buscar_interessados(categoria)
    if not interessados:
        print(f"[PROMOCOES] Ninguém inscrito na categoria {categoria}.")
        return

    assunto = f"Promoção categoria {categoria}: {desconto}% OFF"
    html = montar_html(categoria, descricao, desconto)

    # Um e-mail por pessoa, pra ninguém ver o endereço dos outros.
    enviados = sum(enviar_email(email, assunto, html) for email in interessados)
    print(f"[PROMOCOES] {enviados}/{len(interessados)} e-mails enviados.")


# ---------------------------------------------------------------- Menu
def menu():
    while True:
        print("\n===== MS PROMOÇÕES =====")
        print("1 - Gerar promoção")
        print("2 - Listar inscritos")
        print("0 - Sair")
        opcao = input("> ").strip()

        if opcao == "1":
            categoria = input(f"Categoria {CATEGORIAS}: ").strip().upper()
            if categoria not in CATEGORIAS:
                print("Categoria inválida.")
                continue
            descricao = input("Descrição da promoção: ").strip() or "Ofertas imperdíveis!"
            desconto = input("Desconto (%): ").strip()
            if not desconto.isdigit():
                print("Desconto inválido.")
                continue
            gerar_promocao(categoria, descricao, desconto)

        elif opcao == "2":
            inscritos = listar_interesses()
            if not inscritos:
                print("Nenhum inscrito.")
            for email, categoria in inscritos:
                print(f"  {email:35} {categoria}")

        elif opcao == "0":
            print("Encerrando...")
            os._exit(0)


if __name__ == "__main__":
    if not RESEND_API_KEY:
        print("[PROMOCOES] Aviso: RESEND_API_KEY não definida; os e-mails vão falhar.")

    inicializar_banco()

    # Thread 1 (fundo): escuta o RabbitMQ. Thread principal: menu.
    threading.Thread(target=iniciar_consumo, daemon=True).start()
    print("[PROMOCOES] Aguardando eventos do RabbitMQ...")
    menu()