# Trabalho 1 — Sistemas Distribuídos

Repositório com os microsserviços e consumidores do trabalho, utilizando RabbitMQ para a comunicação entre processos.

## Instalação

É necessário ter o [uv](https://docs.astral.sh/uv/) e o RabbitMQ instalados e em execução localmente (`localhost`).

Na raiz do projeto:

```bash
uv venv
uv pip install -r requirements.txt
```

## Execução

Entre em `src` e execute o processo desejado como módulo. Por exemplo:

```bash
cd src
uv run python -m backend.consumidores.consumidor_1
uv run python -m backend.consumidores.consumidor_2
uv run python -m backend.main.main
uv run python -m backend.estoque.estoque
uv run python -m backend.pagamento.pagamento
uv run python -m backend.entrega.entrega
uv run python -m backend.promocoes.promocoes
```

O RabbitMQ deve estar ativo antes da execução dos consumidores e microsserviços.
