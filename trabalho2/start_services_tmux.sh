#!/usr/bin/env bash

set -euo pipefail

SESSION_NAME="${1:-ecommerce}"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$PROJECT_DIR/src"

if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux não está instalado." >&2
    exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
    echo "uv não está instalado ou não está no PATH." >&2
    exit 1
fi

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
    echo "A sessão '$SESSION_NAME' já existe. Anexe com: tmux attach -t '$SESSION_NAME'" >&2
    exit 1
fi

start_window() {
    local window_name="$1"
    local command="$2"

    if [[ -z "${FIRST_WINDOW:-}" ]]; then
        FIRST_WINDOW="$(tmux new-session -d -P -F '#{window_id}' -s "$SESSION_NAME" \
            -n "$window_name" -c "$SRC_DIR" "$command")"
    else
        tmux new-window -d -t "$SESSION_NAME" -n "$window_name" -c "$SRC_DIR" "$command" >/dev/null
    fi
}

start_window gateway "uv run uvicorn backend.main.main:app --host 127.0.0.1 --port 8000"
start_window estoque "uv run python -m backend.estoque.estoque"
start_window pagamento "uv run python -m backend.pagamento.pagamento"
start_window entrega "uv run python -m backend.entrega.entrega"
start_window promocoes "uv run python -m backend.promocoes.promocoes"
start_window mock-pagamento "uv run python -m mock_pagamento.app_pagamento"
start_window frontend "uv run python -m http.server 8080 --bind 127.0.0.1 --directory frontend"

tmux select-window -t "$SESSION_NAME:gateway"
echo "Serviços iniciados na sessão tmux '$SESSION_NAME'."
echo "Frontend: http://127.0.0.1:8080"
echo "Gateway:  http://127.0.0.1:8000/docs"
echo "Mock:     http://127.0.0.1:8003"
echo "Use Ctrl-b seguido de n/p para trocar de serviço; Ctrl-b d para desanexar."
tmux attach-session -t "$SESSION_NAME"
