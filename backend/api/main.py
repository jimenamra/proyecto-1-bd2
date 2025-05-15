from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from core.command_parser import CommandParser
from core.dispatcher import CommandDispatcher

app = FastAPI(title="API Urbana con Índices Inteligentes")

parser = CommandParser()
dispatcher = CommandDispatcher()

class InstructionRequest(BaseModel):
    texto: str

class SQLRequest(BaseModel):
    query: str

@app.post("/instruccion")
def procesar_instruccion(req: InstructionRequest):
    try:
        comando = parser.parse(req.texto)
        resultado = dispatcher.execute(comando)
        return {"resultado": resultado}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/consulta-sql")
def consulta_sql(req: SQLRequest):
    try:
        eventos = dispatcher.execute_sql(req.query)
        return {"eventos": [e.__dict__ for e in eventos]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/eventos")
def listar_eventos():
    return {"eventos": [e.__dict__ for e in dispatcher.eventos.values()]}
