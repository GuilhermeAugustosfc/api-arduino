from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from service import (
    get_disponibilidade,
    get_intervalos_falhas,
    get_produtividade,
    total_produtos_produzidos,
    get_historico,
)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)


@app.get("/disponibilidade/")
async def disponibilidade(
    timestamp_inicial: str = Query(...), timestamp_final: str = Query(...)
):
    dispo = get_disponibilidade(timestamp_inicial, timestamp_final)
    return dispo


@app.get("/produtividade/")
async def produtividade(
    timestamp_inicial: str = Query(...), timestamp_final: str = Query(...)
):
    produ = get_produtividade(timestamp_inicial, timestamp_final)
    return produ


@app.get("/total_products_produced/")
async def total_products_produced(
    timestamp_inicial: str = Query(...), timestamp_final: str = Query(...)
):
    total_produzidos = total_produtos_produzidos(timestamp_inicial, timestamp_final)
    return total_produzidos


@app.get("/intervalo_falhas/")
async def machine_failures(
    timestamp_inicial: str = Query(...), timestamp_final: str = Query(...)
):
    eventos_falha = get_intervalos_falhas(timestamp_inicial, timestamp_final)
    return eventos_falha


@app.get("/historico/")
async def machine_failures(
    timestamp_inicial: str = Query(...), timestamp_final: str = Query(...)
):
    intervalos = get_historico(timestamp_inicial, timestamp_final)
    return intervalos


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
