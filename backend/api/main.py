from fastapi import FastAPI, HTTPException
from isam_backend import isam, Registro
from parser_sql import parse_sql

app = FastAPI()

@app.post("/query/")
def execute_sql(sql: str):
    op = parse_sql(sql)
    if op[0] == "insert":
        isam.add(Registro(op[1]))
        return {"message": f"Insertado {op[1]}"}
    elif op[0] == "search":
        result = [r.val for r in isam.search(op[1])]
        return {"result": result}
    elif op[0] == "range_search":
        result = [r.val for r in isam.rangeSearch(op[1], op[2])]
        return {"result": result}
    elif op[0] == "delete":
        isam.remove(op[1])
        return {"message": f"Eliminado {op[1]}"}
    else:
        raise HTTPException(status_code=400, detail="Consulta SQL no válida.")
