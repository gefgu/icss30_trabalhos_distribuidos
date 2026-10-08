# Trabalho 2 — Sistemas Distribuídos

Aplicação de e-commerce organizada em microsserviços. O frontend em HTML, CSS e JavaScript acessa o API Gateway por REST e acompanha pedidos por SSE. O RabbitMQ transporta os eventos entre Gateway, Estoque, Pagamento, Entrega e Promoções. O Mock de Pagamento simula um provedor externo e retorna a decisão ao serviço de Pagamento por webhook.

## Componentes

- **Frontend:** catálogo, carrinho, acompanhamento do pedido, inscrição e cancelamento de promoções.
- **API Gateway:** API REST, consulta de produtos no Estoque e SSE para atualizações dos pedidos. O consumidor RabbitMQ do Gateway inicia junto com a aplicação FastAPI.
- **Estoque:** consulta REST de produtos, reserva de itens e persistência do catálogo em `src/backend/estoque/produtos.json` e das reservas em `src/backend/estoque/reservas.json`.
- **Pagamento:** solicita checkout ao Mock e recebe o resultado por webhook.
- **Entrega:** simula emissão de nota fiscal e despacho após pagamento aprovado.
- **Promoções:** persiste interesses e permite gerar campanhas pelo menu do serviço, enviando e-mails pela API Resend.
- **Mock de Pagamento:** checkout web com opções para aprovar ou recusar a cobrança.

## Requisitos

- Python 3.12 ou compatível
- [`uv`](https://docs.astral.sh/uv/)
- `tmux` para iniciar os serviços em uma sessão única
- RabbitMQ disponível em `localhost`
- Uma chave Resend (`RESEND_API_KEY`) para envio real de e-mails

## Instalação

Na raiz do repositório:

```bash
uv venv
uv pip install --python .venv/bin/python -r requirements.txt
```

## Execução

Inicie o RabbitMQ antes dos serviços. Para subir os processos em janelas separadas do tmux, preencha `RESEND_API_KEY` no arquivo `.env` da raiz e execute:

```bash
./start_services_tmux.sh
```

O script cria uma única janela com painéis para Gateway, Estoque, Pagamento, Entrega, Promoções, Mock de Pagamento e frontend, e anexa à sessão `ecommerce`. Se algum processo encerrar, o painel permanece aberto para mostrar o erro. Use `Ctrl-b` seguido das setas para navegar entre painéis, `Ctrl-b z` para ampliar/recolher o painel atual e `Ctrl-b d` para desanexar. Para escolher outro nome de sessão, passe-o como argumento, por exemplo `./start_services_tmux.sh minha-loja`.

Para iniciar os processos manualmente, mantenha um terminal aberto para cada comando abaixo. Execute os comandos a partir da raiz do repositório.

### API Gateway e consumidor de eventos

```bash
cd src
uv run uvicorn backend.main.main:app --host 127.0.0.1 --port 8000
```

O consumidor RabbitMQ do Gateway é iniciado pelo próprio processo FastAPI. A documentação REST fica em <http://127.0.0.1:8000/docs>.

### Microsserviços

Em terminais separados, também a partir de `src`:

```bash
uv run python -m backend.estoque.estoque
```

```bash
uv run python -m backend.pagamento.pagamento
```

```bash
uv run python -m backend.entrega.entrega
```

```bash
uv run python -m backend.promocoes.promocoes
```

O serviço lê `RESEND_API_KEY` do `.env` na raiz ou do ambiente do shell, e inicia um menu interativo para gerar campanhas e listar inscrições. Sem uma chave válida, o fluxo de cadastro funciona, mas o envio de e-mail real não.

### Mock de Pagamento

```bash
cd src
uv run python -m mock_pagamento.app_pagamento
```

O Mock fica em <http://127.0.0.1:8003>. O checkout é aberto após o Estoque confirmar o pedido e o serviço de Pagamento solicitar a cobrança.

### Frontend da loja

Em outro terminal:

```bash
cd src
uv run python -m http.server 8080 --bind 127.0.0.1 --directory frontend
```

Abra <http://127.0.0.1:8080>. O JavaScript do frontend está configurado para acessar o Gateway em `http://127.0.0.1:8000`.

## Rotas principais

| Método | Rota | Uso |
|---|---|---|
| `GET` | `/produtos` | Consulta produtos disponíveis no serviço de Estoque |
| `POST` | `/pedido` | Cria um pedido e publica `pedido.criado` |
| `GET` | `/pedidos` | Lista o histórico de pedidos persistidos |
| `GET` | `/pedidos/{id}/status` | Recebe atualizações do pedido por SSE |
| `POST` | `/interesse` | Registra interesse em promoções |
| `DELETE` | `/interesse` | Cancela interesse em promoções |
| `POST` | `/webhook/pagamento` | Recebe a decisão do Mock de Pagamento |

## Observações sobre o estado atual

- As mensagens RabbitMQ ainda não têm assinatura digital implementada.
- O catálogo e as reservas do Estoque são persistidos em JSON.
- O Mock mantém os pagamentos em memória, então seus dados são perdidos quando o processo reinicia.
- As URLs entre serviços usam `localhost` e as portas `8000` a `8003` conforme os comandos acima.
