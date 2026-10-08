import httpx
import uvicorn
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


FRONTEND_FOLDER = Path(__file__).resolve().parents[1] / "frontend"
app = FastAPI()
pagamentos = {}
app.mount("/frontend", StaticFiles(directory=FRONTEND_FOLDER), name="frontend")

@app.get("/")
async def home():
    return {"message": "Mock de pagamento está rodando."}


@app.post("/app_pagamento")
async def criar_pagamento(pedido_id: int, webhook_url: str):
    pagamentos[pedido_id] = {"pedido_id": pedido_id, "status": "pendente", "webhook_url": webhook_url}
    checkout_url = f"http://localhost:8003/app_pagamento/checkout/{pedido_id}"
    return {"pedido_id": pedido_id, "status": "pendente", "checkout_url": checkout_url}


@app.get("/app_pagamento/checkout/{pedido_id}", response_class=HTMLResponse)
async def checkout(pedido_id: int):
    pagamento = pagamentos.get(pedido_id)

    if pagamento is None:
        return HTMLResponse(
            content="Pagamento não encontrado",
            status_code=404
        )

    return FileResponse(FRONTEND_FOLDER / "checkout.html")


@app.post("/app_pagamento/{pedido_id}/aprovar")
async def aprovar_pagamento(pedido_id: int):
    pagamento = pagamentos.get(pedido_id)

    if pagamento is None:
        return {"erro": "Pagamento não encontrado"}

    pagamento["status"] = "APROVADO"

    async with httpx.AsyncClient() as client:
        await client.post(
            pagamento["webhook_url"],
            json={
                "pedido_id": pedido_id,
                "status": "APROVADO"
            }
        )

    return {
        "pedido_id": pedido_id,
        "status": "APROVADO"
    }

@app.post("/app_pagamento/{pedido_id}/recusar")
async def recusar_pagamento(pedido_id: int):
    pagamento = pagamentos.get(pedido_id)

    if pagamento is None:
        return {"erro": "Pagamento não encontrado"}

    pagamento["status"] = "RECUSADO"

    async with httpx.AsyncClient() as client:
        await client.post(
            pagamento["webhook_url"],
            json={
                "pedido_id": pedido_id,
                "status": "RECUSADO"
            }
        )

    return {
        "pedido_id": pedido_id,
        "status": "RECUSADO"
    }

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8003)