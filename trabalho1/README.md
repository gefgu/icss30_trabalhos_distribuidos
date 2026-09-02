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
uv run python -m consumidores.consumidores
```

O RabbitMQ deve estar ativo antes da execução dos consumidores e microsserviços.
