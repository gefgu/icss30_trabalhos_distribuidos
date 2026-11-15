#!/usr/bin/env bash
# Abre os 4 nós e o cliente numa janela do tmux, dividida em 5 painéis.
#
#   ./start_tmux.sh           -> os 4 nós normais
#   ./start_tmux.sh lento     -> nó 4 com Fetch a cada 5 s (Cenário 5) e um
#                                segundo painel "Cliente 2", para consultar
#                                enquanto o 1º cliente espera
#   ./start_tmux.sh limpo     -> apaga a pasta dados/ antes (começa do zero)
#   ./start_tmux.sh limpo lento
#
# Navegar: Ctrl+B e depois uma seta muda de painel.
# Fechar tudo: Ctrl+B, depois digite  :kill-session  e Enter.

SESSAO="trabalho3"
PASTA="$(cd "$(dirname "$0")" && pwd)"
# Escolhe o interpretador antes de abrir os painéis: o tmux pode usar outro PATH.
if [ -x "$PASTA/.venv/bin/python" ]; then
  PYTHON="$PASTA/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PYTHON="$(command -v python)"
else
  echo "Python não encontrado. Instale Python 3 e as dependências do README." >&2
  exit 1
fi
if ! "$PYTHON" -c 'import grpc, google.protobuf' >/dev/null 2>&1; then
  echo "Dependências ausentes para $PYTHON. Instale-as conforme o README." >&2
  exit 1
fi
# Escapa caminhos para o shell de cada painel, inclusive espaços e aspas.
printf -v PASTA_ESCAPADA '%q' "$PASTA/src"
printf -v PYTHON_ESCAPADO '%q' "$PYTHON"
ATIVAR="cd $PASTA_ESCAPADA && clear"

EXTRA_NO4=""
for arg in "$@"; do
  case "$arg" in
    lento) EXTRA_NO4="--intervalo-fetch 5000" ;;
    limpo) rm -rf "$PASTA/dados" && echo "Pasta dados/ apagada." ;;
  esac
done

if ! command -v tmux >/dev/null; then
  echo "tmux não encontrado. Instale com: brew install tmux"
  exit 1
fi

tmux kill-session -t "$SESSAO" 2>/dev/null

# Painel 0: nó 1
tmux new-session -d -s "$SESSAO" -x 240 -y 60

# Aparência: mouse liga clique para escolher painel e rolagem; título em cada painel.
tmux set -t "$SESSAO" mouse on
tmux set -t "$SESSAO" pane-border-status top
tmux set -t "$SESSAO" pane-border-format " #{pane_title} "
tmux set -t "$SESSAO" pane-active-border-style "fg=green,bold"
tmux set -t "$SESSAO" status-style "bg=colour236,fg=white"
tmux set -t "$SESSAO" status-left " Trabalho 3 "
tmux set -t "$SESSAO" status-right " Ctrl+B seta: muda painel | Ctrl+B :kill-session  "
tmux set -t "$SESSAO" status-right-length 60
tmux set -t "$SESSAO" allow-rename off
tmux select-pane -T "Nó 1"
tmux send-keys -t "$SESSAO" "$ATIVAR && $PYTHON_ESCAPADO -m backend.cluster.cluster --id 1" C-m

# Painéis 1 a 3: nós 2, 3 e 4
tmux split-window -h -t "$SESSAO"
tmux select-pane -T "Nó 2"
tmux send-keys -t "$SESSAO" "$ATIVAR && $PYTHON_ESCAPADO -m backend.cluster.cluster --id 2" C-m
tmux split-window -v -t "$SESSAO:0.0"
tmux select-pane -T "Nó 3"
tmux send-keys -t "$SESSAO" "$ATIVAR && $PYTHON_ESCAPADO -m backend.cluster.cluster --id 3" C-m
tmux split-window -v -t "$SESSAO:0.2"
tmux select-pane -T "Nó 4${EXTRA_NO4:+ (lento)}"
tmux send-keys -t "$SESSAO" "$ATIVAR && $PYTHON_ESCAPADO -m backend.cluster.cluster --id 4 $EXTRA_NO4" C-m

# Painel de baixo, largura inteira: cliente
tmux split-window -v -f -l 15 -t "$SESSAO"
tmux select-pane -T "Cliente"
tmux send-keys -t "$SESSAO" "$ATIVAR && sleep 3 && $PYTHON_ESCAPADO -m backend.cliente.cliente" C-m

# Modo lento: segundo cliente ao lado do primeiro (Cenário 5, passo 5).
if [ -n "$EXTRA_NO4" ]; then
  tmux split-window -h -t "$SESSAO"
  tmux select-pane -T "Cliente 2"
  tmux send-keys -t "$SESSAO" "$ATIVAR && sleep 3 && $PYTHON_ESCAPADO -m backend.cliente.cliente" C-m
fi

tmux attach -t "$SESSAO"
