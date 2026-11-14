#!/usr/bin/env bash
# Abre os 4 nós e o cliente numa janela do tmux, dividida em 5 painéis.
#
#   ./start_tmux.sh           -> os 4 nós normais
#   ./start_tmux.sh lento     -> nó 4 com Fetch a cada 5 s (Cenário 5)
#   ./start_tmux.sh limpo     -> apaga a pasta dados/ antes (começa do zero)
#   ./start_tmux.sh limpo lento
#
# Navegar: Ctrl+B e depois uma seta muda de painel.
# Fechar tudo: Ctrl+B, depois digite  :kill-session  e Enter.

SESSAO="trabalho3"
PASTA="$(cd "$(dirname "$0")" && pwd)"
# "clear" apaga o comando comprido antes de o programa começar.
ATIVAR="cd '$PASTA/src' && { [ -f '$PASTA/.venv/bin/activate' ] && source '$PASTA/.venv/bin/activate'; true; } && clear"

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
tmux send-keys -t "$SESSAO" "$ATIVAR && python -m backend.cluster.cluster --id 1" C-m

# Painéis 1 a 3: nós 2, 3 e 4
tmux split-window -h -t "$SESSAO"
tmux select-pane -T "Nó 2"
tmux send-keys -t "$SESSAO" "$ATIVAR && python -m backend.cluster.cluster --id 2" C-m
tmux split-window -v -t "$SESSAO:0.0"
tmux select-pane -T "Nó 3"
tmux send-keys -t "$SESSAO" "$ATIVAR && python -m backend.cluster.cluster --id 3" C-m
tmux split-window -v -t "$SESSAO:0.2"
tmux select-pane -T "Nó 4${EXTRA_NO4:+ (lento)}"
tmux send-keys -t "$SESSAO" "$ATIVAR && python -m backend.cluster.cluster --id 4 $EXTRA_NO4" C-m

# Painel de baixo, largura inteira: cliente
tmux split-window -v -f -l 15 -t "$SESSAO"
tmux select-pane -T "Cliente"
tmux send-keys -t "$SESSAO" "$ATIVAR && sleep 3 && python -m backend.cliente.cliente" C-m

tmux attach -t "$SESSAO"
