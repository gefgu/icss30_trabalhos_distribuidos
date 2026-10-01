# Trabalho 2 — Sistemas Distribuídos

Projeto de e-commerce com uma API FastAPI, microsserviços e RabbitMQ.

## Requisitos

- Python 3.12 ou compatível
- [uv](https://docs.astral.sh/uv/)
- RabbitMQ em execução local, acessível em `localhost`

## Instalação

Na raiz do projeto (`trabalho2`):

```bash
uv venv
uv pip install -r requirements.txt
```

## Execução

Inicie o RabbitMQ primeiro. Abra terminais separados na raiz do projeto e execute os processos abaixo.

### API FastAPI

```bash
cd src
uv run uvicorn backend.main.main:app --reload
```

A documentação interativa fica em <http://127.0.0.1:8000/docs> e a rota inicial em <http://127.0.0.1:8000/>.

### Consumidor do API Gateway

Em outro terminal, na raiz do projeto:

```bash
cd src
uv run python -m backend.main.main
```

### Microsserviços

Execute cada microsserviço em um terminal separado, também a partir de `src`:

```bash
uv run python -m backend.estoque.estoque
uv run python -m backend.pagamento.pagamento
uv run python -m backend.entrega.entrega
uv run python -m backend.promocoes.promocoes
```

Para testar somente a API, basta iniciar o RabbitMQ se alguma operação depender dele, o consumidor do API Gateway e a API FastAPI. Os endpoints disponíveis podem ser explorados em `/docs`.
