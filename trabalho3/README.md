# Trabalho 3: Replicação de dados com gRPC (Pull + eleição estilo Raft)

5 processos: 4 nós do cluster (mesmo código) e 1 cliente. Tudo via gRPC.

## Instalar (uma vez)
```bash
cd trabalho3
uv venv && source .venv/bin/activate
uv pip install -r requirements.txt
```

## Rodar tudo de uma vez (tmux)
Precisa do tmux (`brew install tmux`, só uma vez). Dentro de `trabalho3`:
```bash
./start_tmux.sh              # 4 nós + cliente numa janela só
./start_tmux.sh limpo        # o mesmo, apagando dados/ antes
./start_tmux.sh limpo lento  # nó 4 lento, para o Cenário 5
```
Trocar de painel: Ctrl+B e uma seta. Fechar tudo: Ctrl+B, `:kill-session`, Enter.

## Ou rodar à mão: 5 terminais, todos dentro de `trabalho3/src`
```bash
python -m backend.cluster.cluster --id 1
python -m backend.cluster.cluster --id 2
python -m backend.cluster.cluster --id 3
python -m backend.cluster.cluster --id 4
python -m backend.cliente.cliente
```
Para o Cenário 5 (nó lento, ver `GUIA_DEFESA.md`): `python -m backend.cluster.cluster --id 4 --intervalo-fetch 5000`

Para derrubar um nó: Ctrl+C no terminal dele. Para religar: o mesmo comando.
Para começar do zero: apague a pasta `trabalho3/dados`.

## Regerar o código gRPC (só se mudar o .proto), dentro de `src`
```bash
python -m grpc_tools.protoc -I . --python_out=. --pyi_out=. --grpc_python_out=. backend/proto/service.proto
```

## Arquivos
| Arquivo | Papel |
| --- | --- |
| `src/backend/proto/service.proto` | contrato gRPC (mensagens e serviços) |
| `src/backend/proto/service_pb2*.py` | gerados pelo protoc, não editar |
| `src/backend/cluster/cluster.py` | o nó: eleição, Fetch, quórum, commit, truncamento, persistência |
| `src/backend/cliente/cliente.py` | cliente com menu (gravar, consultar, cenário B) |
| `dados/no_<id>/` | estado.json (termo, voted_for), log.json, banco.json |
