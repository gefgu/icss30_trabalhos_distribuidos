#!/usr/bin/env bash

set -euo pipefail

SESSION_NAME="${1:-ecommerce}"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$PROJECT_DIR/src"
SESSION_CREATED=false

cleanup_on_error() {
    if [[ "$SESSION_CREATED" == true ]]; then
        tmux kill-session -t "$SESSION_NAME" 2>/dev/null || true
    fi
}

trap cleanup_on_error ERR

if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux não está instalado."
    exit 1
fi

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
    echo "A sessão '$SESSION_NAME' já existe."
    exit 1
fi

MAIN_PANE=$(tmux new-session -d -P -F '#{pane_id}' -s "$SESSION_NAME" \
    -n services -c "$SRC_DIR" "exec uv run python -m backend.main.main")
SESSION_CREATED=true

# Cria a linha inferior antes de dividir a linha superior.
DELIVERY_PANE=$(tmux split-window -v -l 50% -P -F '#{pane_id}' -t "$MAIN_PANE" \
    -c "$SRC_DIR" "exec uv run python -m backend.entrega.entrega")

# Linha superior: main ocupa duas das quatro colunas.
STOCK_PANE=$(tmux split-window -h -l 50% -P -F '#{pane_id}' -t "$MAIN_PANE" \
    -c "$SRC_DIR" "exec uv run python -m backend.estoque.estoque")
PAYMENT_PANE=$(tmux split-window -h -l 50% -P -F '#{pane_id}' -t "$STOCK_PANE" \
    -c "$SRC_DIR" "exec uv run python -m backend.pagamento.pagamento")

# Linha inferior: quatro painéis do mesmo tamanho.
PROMOTIONS_PANE=$(tmux split-window -h -l 75% -P -F '#{pane_id}' -t "$DELIVERY_PANE" \
    -c "$SRC_DIR" "exec uv run python -m backend.promocoes.promocoes")
CONSUMER_1_PANE=$(tmux split-window -h -l 66% -P -F '#{pane_id}' -t "$PROMOTIONS_PANE" \
    -c "$SRC_DIR" "exec uv run python -m backend.consumidores.consumidor_1")
CONSUMER_2_PANE=$(tmux split-window -h -l 50% -P -F '#{pane_id}' -t "$CONSUMER_1_PANE" \
    -c "$SRC_DIR" "exec uv run python -m backend.consumidores.consumidor_2")

tmux set-option -t "$SESSION_NAME" pane-border-status top
tmux set-option -t "$SESSION_NAME" pane-border-format ' #{pane_title} '
tmux select-pane -t "$MAIN_PANE" -T main
tmux select-pane -t "$STOCK_PANE" -T estoque
tmux select-pane -t "$PAYMENT_PANE" -T pagamento
tmux select-pane -t "$DELIVERY_PANE" -T entrega
tmux select-pane -t "$PROMOTIONS_PANE" -T promocoes
tmux select-pane -t "$CONSUMER_1_PANE" -T consumidor_1
tmux select-pane -t "$CONSUMER_2_PANE" -T consumidor_2
tmux select-pane -t "$MAIN_PANE"
trap - ERR
tmux attach-session -t "$SESSION_NAME"
