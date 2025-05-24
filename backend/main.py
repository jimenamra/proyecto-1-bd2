from fastapi import FastAPI, Query
from pydantic import BaseModel
from ISAM.isam import Registro,ISAM
from .sql_parser import execute_sql

app = FastAPI()
isam = ISAM()

class InsertRequest(BaseModel):
    value: int

@app.get("/search/{key}")
def search(key: int):
    return {"result": isam.search(key)}

@app.get("/range/{a}/{b}")
def range_search(a: int, b: int):
    return {"result": isam.rangeSearch(a, b)}

@app.post("/insert")
def insert(data: InsertRequest):
    reg = Registro(data.value)
    isam.add(reg)
    return {"message": f"Insertado {data.value}"}

@app.delete("/delete/{key}")
def delete(key: int):
    isam.remove(key)
    return {"message": f"Eliminado {key}"}

@app.post("/sql")
def sql_query(sql: str = Query(...)):
    result = execute_sql(sql)
    return {"result": result}
