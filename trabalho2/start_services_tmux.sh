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
    echo "tmux não está instalado." >&2
    exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
    echo "uv não está instalado ou não está no PATH." >&2
    exit 1
fi

PYTHON="$PROJECT_DIR/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
    echo "Ambiente virtual não encontrado em .venv. Crie-o com: uv venv" >&2
    exit 1
fi

if ! "$PYTHON" -c 'import fastapi, httpx, pika, uvicorn' >/dev/null 2>&1; then
    echo "Dependências do projeto ausentes no ambiente .venv." >&2
    echo "Instale-as na raiz com: uv pip install --python .venv/bin/python -r requirements.txt" >&2
    exit 1
fi

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
    echo "A sessão '$SESSION_NAME' já existe. Anexe com: tmux attach -t '$SESSION_NAME'" >&2
    exit 1
fi

pane_command() {
    local pane_name="$1"
    local command="$2"
    local quoted_src
    printf -v quoted_src '%q' "$SRC_DIR"

    # Se o serviço falhar ou for interrompido, deixa um shell aberto no painel
    # para que a mensagem de erro não desapareça.
    printf 'cd %s || exec bash -i; echo "[%s] starting: %s"; %s; service_exit=$?; echo; echo "[%s] exited with code $service_exit; shell remains open"; exec bash -i' \
        "$quoted_src" "$pane_name" "$command" "$command" "$pane_name"
}

MAIN_PANE="$(tmux new-session -d -P -F '#{pane_id}' -s "$SESSION_NAME" \
    -n services -c "$SRC_DIR" \
    "$(pane_command gateway 'uv run uvicorn backend.main.main:app --host 127.0.0.1 --port 8000')")"
SESSION_CREATED=true
tmux select-pane -t "$MAIN_PANE" -T gateway

add_pane() {
    local pane_name="$1"
    local command="$2"
    local pane_info target width height split_direction new_pane

    # Divide o painel com maior área para manter a grade equilibrada.
    pane_info="$(tmux list-panes -t "$SESSION_NAME:services" \
        -F '#{pane_id} #{pane_width} #{pane_height}' \
        | awk 'BEGIN { max = -1 } { area = $2 * $3; if (area > max) { max = area; line = $0 } } END { print line }')"
    read -r target width height <<< "$pane_info"

    if (( width >= height )); then
        split_direction="-h"
    else
        split_direction="-v"
    fi

    new_pane="$(tmux split-window -d "$split_direction" -P -F '#{pane_id}' \
        -t "$target" -c "$SRC_DIR" "$(pane_command "$pane_name" "$command")")"
    tmux select-pane -t "$new_pane" -T "$pane_name"
    tmux select-layout -t "$SESSION_NAME:services" tiled
}

add_pane estoque "uv run python -m backend.estoque.estoque"
add_pane pagamento "uv run python -m backend.pagamento.pagamento"
add_pane entrega "uv run python -m backend.entrega.entrega"
add_pane promocoes "uv run python -m backend.promocoes.promocoes"
add_pane mock-pagamento "uv run python -m mock_pagamento.app_pagamento"
add_pane frontend "uv run python -m http.server 8080 --bind 127.0.0.1 --directory frontend"

tmux set-window-option -t "$SESSION_NAME:services" pane-border-status top
tmux set-window-option -t "$SESSION_NAME:services" pane-border-format ' #{pane_title} '
tmux select-pane -t "$MAIN_PANE"
trap - ERR
echo "Serviços iniciados em painéis na janela 'services' da sessão '$SESSION_NAME'."
echo "Frontend: http://127.0.0.1:8080"
echo "Gateway:  http://127.0.0.1:8000/docs"
echo "Mock:     http://127.0.0.1:8003"
echo "Use Ctrl-b seguido de setas para navegar entre painéis; Ctrl-b z amplia/recolhe um painel; Ctrl-b d desanexa."
tmux attach-session -t "$SESSION_NAME"
