from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime

from service import (
    get_disponibilidade,
    get_intervalos_falhas,
    get_produtividade,
    total_produtos_produzidos,
    get_historico,
    get_config_maquina,
    update_motivo,
    config_maquina,
    get_quantidade_motivos,
    get_user,
    get_timelapse,
    save_apontamento,
    pecas_perdidas,
    pecas_perdidas_range,
    processar_timelapse,
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


@app.get("/quantidade_motivos/")
async def quantidade_motivos(
    timestamp_inicial: str = Query(...), timestamp_final: str = Query(...)
):
    quantidade_motivo = get_quantidade_motivos(timestamp_inicial, timestamp_final)
    return quantidade_motivo


class salvarApontamento(BaseModel):
    data: str
    pecasDefeituosas: int


@app.post("/salvar-apontamento")
async def salvar_apontamento(request: salvarApontamento):
    try:
        update_rows = save_apontamento(request)

        if update_rows == -1:
            raise HTTPException(
                status_code=500, detail="Erro ao atualizar o banco de dados"
            )

        if update_rows == 0:
            raise HTTPException(
                status_code=404, detail="Nenhum registro encontrado para atualizar"
            )

        return update_rows

    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Erro ao atualizar",
        )


@app.get("/produtividade/")
async def produtividade(
    timestamp_inicial: str = Query(...), timestamp_final: str = Query(...)
):
    produ = get_produtividade(timestamp_inicial, timestamp_final)
    return produ


@app.get("/timelapse/")
async def timelapse(
    timestamp_inicial: str = Query(...), timestamp_final: str = Query(...)
):
    dados = get_timelapse(timestamp_inicial, timestamp_final)
    response = processar_timelapse(dados)
    return response


@app.get("/total_products_produced/")
async def total_products_produced(
    timestamp_inicial: str = Query(...), timestamp_final: str = Query(...)
):
    total_produzidos = total_produtos_produzidos(timestamp_inicial, timestamp_final)
    return total_produzidos


@app.get("/get_pecas_perdidas/")
async def get_pecas_perdidas(date: str = Query(...)):
    response = pecas_perdidas(date)
    return response


@app.get("/get_pecas_perdidas_range/")
async def get_pecas_perdidas_range(
    date_inicial: str = Query(...), date_final: str = Query(...)
):
    response = pecas_perdidas_range(date_inicial, date_final)
    return response


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


class LoginRequest(BaseModel):
    login: str
    password: str


@app.post("/login/")
async def login(request: LoginRequest):
    user = get_user(request.login, request.password)
    return user


class MotivoRequest(BaseModel):
    timestamp_inicial: str
    timestamp_final: str
    motivo: int


@app.post("/motivo/")
async def motivo(request: MotivoRequest):
    try:
        # Convertendo strings para objetos datetime para validar o formato e a ordem
        timestamp_inicial = datetime.fromisoformat(request.timestamp_inicial)
        timestamp_final = datetime.fromisoformat(request.timestamp_final)

        # Verificando se timestamp_final é posterior ao timestamp_inicial
        if timestamp_final <= timestamp_inicial:
            raise HTTPException(
                status_code=400,
                detail="timestamp_final deve ser posterior ao timestamp_inicial",
            )

        update_rows = update_motivo(
            request.timestamp_inicial, request.timestamp_final, request.motivo
        )

        if update_rows == -1:
            raise HTTPException(
                status_code=500, detail="Erro ao atualizar o banco de dados"
            )

        if update_rows == 0:
            raise HTTPException(
                status_code=404, detail="Nenhum registro encontrado para atualizar"
            )
        return update_rows
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Formato de timestamp inválido. Use o formato ISO 8601.",
        )


class ConfigMaquina(BaseModel):
    total_produto: int
    horario_max_manutencao: int
    total_horas_trabalho: int
    timeToCicleProducting: int
    pricePerPiece: float


@app.get("/config-maquina/")
async def motivo(id: str = Query(...)):
    config = get_config_maquina(id)
    return config


@app.post("/config-maquina/")
async def motivo(request: ConfigMaquina):
    try:
        update_rows = config_maquina(
            request.total_produto,
            request.total_horas_trabalho,
            request.horario_max_manutencao,
            request.timeToCicleProducting,
            request.pricePerPiece,
        )

        if update_rows == -1:
            raise HTTPException(
                status_code=500, detail="Erro ao atualizar o banco de dados"
            )

        if update_rows == 0:
            raise HTTPException(
                status_code=404, detail="Nenhum registro encontrado para atualizar"
            )

        return update_rows

    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Erro ao atualizar",
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
