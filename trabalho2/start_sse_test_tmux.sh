#!/usr/bin/env bash

set -euo pipefail

SESSION_NAME="${1:-ecommerce-sse-test}"
API_PORT="${2:-8001}"
API_URL="http://127.0.0.1:$API_PORT"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$PROJECT_DIR/src"
DB_PATH="$SRC_DIR/backend/main/main.db"
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
    tmux kill-session -t "$SESSION_NAME"
fi

ORDER_ID="$(python3 -c 'import sqlite3, sys
try:
    con = sqlite3.connect(sys.argv[1])
    row = con.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM pedidos").fetchone()
    print(row[0])
except sqlite3.Error:
    print(1)' "$DB_PATH")"

API_PANE=$(tmux new-session -d -P -F '#{pane_id}' -s "$SESSION_NAME" \
    -n services -c "$SRC_DIR" \
    "exec uv run uvicorn backend.main.main:app --reload --port $API_PORT"
)
SESSION_CREATED=true
tmux set-option -t "$SESSION_NAME" remain-on-exit on
DELIVERY_PANE=$(tmux split-window -v -l 50% -P -F '#{pane_id}' -t "$API_PANE" \
    -c "$SRC_DIR" "exec uv run python -m backend.entrega.entrega")

STOCK_PANE=$(tmux split-window -h -l 66% -P -F '#{pane_id}' -t "$API_PANE" \
    -c "$SRC_DIR" "exec uv run python -m backend.estoque.estoque"
)
PAYMENT_PANE=$(tmux split-window -h -l 50% -P -F '#{pane_id}' -t "$STOCK_PANE" \
    -c "$SRC_DIR" "exec uv run python -m backend.pagamento.pagamento"
)
SSE_PANE=$(tmux split-window -h -l 66% -P -F '#{pane_id}' -t "$DELIVERY_PANE" \
    -c "$SRC_DIR" "exec curl --retry 30 --retry-connrefused --retry-delay 1 -N $API_URL/pedidos/$ORDER_ID/status")
CREATE_SCRIPT="read -r -p 'Press Enter to create order $ORDER_ID...' _; curl -sS -X POST $API_URL/pedido -H 'Content-Type: application/json' -d '{\"pedidos\":[{\"id\":1,\"nome\":\"Produto\",\"categoria\":\"geral\",\"quantidade\":1}]}' ; echo"
CREATE_COMMAND="bash -c $(printf '%q' "$CREATE_SCRIPT")"
CREATE_PANE=$(tmux split-window -h -l 50% -P -F '#{pane_id}' -t "$SSE_PANE" \
    -c "$SRC_DIR" \
    "$CREATE_COMMAND")

tmux set-option -t "$SESSION_NAME" pane-border-status top
tmux set-option -t "$SESSION_NAME" pane-border-format ' #{pane_title} '
tmux select-pane -t "$API_PANE" -T api
tmux select-pane -t "$STOCK_PANE" -T estoque
tmux select-pane -t "$PAYMENT_PANE" -T pagamento
tmux select-pane -t "$DELIVERY_PANE" -T entrega
tmux select-pane -t "$SSE_PANE" -T sse
tmux select-pane -t "$CREATE_PANE" -T criar-pedido
tmux select-pane -t "$API_PANE"

trap - ERR
echo "SSE will follow order $ORDER_ID on port $API_PORT. Start RabbitMQ before using the services."
tmux attach-session -t "$SESSION_NAME"
