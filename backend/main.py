from fastapi import FastAPI, Query, UploadFile, File, HTTPException
from ISAM.isam import Registro,ISAM
from ISAM.btree import BTree
from .sql_parser import parse_sql
from .rtree import RTreeIndex
from typing import List
from pydantic import BaseModel
import pandas as pd
import os
import json
from time import time

app = FastAPI()
isam = ISAM()
btree = BTree()
#rtree = RTreeIndex()
timing_log = []  # metricas

class RegistroRequest(BaseModel):
    id: int
    fecha: str
    tipo: str
    lat: float
    lon: float
    mag: float
    prof: float

# class InsertRequest(BaseModel):
#     value: int

@app.get("/search/{key}")
def search(key: int):
    response = []

    # ISAM
    t0 = time()
    isam_result = isam.search(key)
    t1 = time()
    response.append({
        "metodo": "ISAM",
        "tiempo": round(t1 - t0, 6),
        "resultados": [r.__dict__ for r in isam_result]
    })

    # B+ Tree
    t0 = time()
    btree_result = btree.search(key)
    t1 = time()
    response.append({
        "metodo": "BTree",
        "tiempo": round(t1 - t0, 6),
        "resultados": btree_result.__dict__ if btree_result else []
    })

    # # RTree (comentado)
    # t0 = time()
    # rtree_result = rtree.search_by_id(key)
    # t1 = time()
    # response.append({
    #     "metodo": "RTree",
    #     "tiempo": round(t1 - t0, 6),
    #     "resultados": rtree_result
    # })

    return response

@app.get("/range/{a}/{b}")
def range_search(a: int, b: int):
    response = []

    # ISAM
    t0 = time()
    isam_result = isam.rangeSearch(a, b)
    t1 = time()
    response.append({
        "metodo": "ISAM",
        "tiempo": round(t1 - t0, 6),
        "resultados": [r.__dict__ for r in isam_result]
    })

    # B+ Tree
    t0 = time()
    btree_result = btree.range_search(a, b)
    t1 = time()
    response.append({
        "metodo": "BTree",
        "tiempo": round(t1 - t0, 6),
        "resultados": [r.__dict__ for r in btree_result]
    })

    # # RTree (comentado)
    # t0 = time()
    # rtree_result = rtree.range_search_area(a, b)
    # t1 = time()
    # response.append({
    #     "metodo": "RTree",
    #     "tiempo": round(t1 - t0, 6),
    #     "resultados": rtree_result
    # })

    return response

@app.post("/insert")
def insert(req: RegistroRequest):
    response = []
    r = Registro(req.id, req.fecha, req.tipo, req.lat, req.lon, req.mag, req.prof)

    t0 = time()
    isam.add(r)
    t1 = time()
    response.append({
        "metodo": "ISAM",
        "tiempo": round(t1 - t0, 6),
        "resultado": f"ID {r.id} insertado"
    })

    t0 = time()
    btree.add(r)
    t1 = time()
    response.append({
        "metodo": "BTree",
        "tiempo": round(t1 - t0, 6),
        "resultado": f"ID {r.id} insertado"
    })

    # # RTree (comentado)
    # t0 = time()
    # rtree.add({
    #     "id": r.id,
    #     "lat": r.lat,
    #     "lon": r.lon,
    #     "descripcion": r.tipo
    # })
    # t1 = time()
    # response.append({
    #     "metodo": "RTree",
    #     "tiempo": round(t1 - t0, 6),
    #     "resultado": f"ID {r.id} insertado"
    # })

    return response



@app.delete("/delete/{id}")
def delete(id: int):
    response = []

    t0 = time()
    isam.remove(id)
    t1 = time()
    response.append({
        "metodo": "ISAM",
        "tiempo": round(t1 - t0, 6),
        "resultado": f"ID {id} eliminado"
    })

    t0 = time()
    btree.remove(id)
    t1 = time()
    response.append({
        "metodo": "BTree",
        "tiempo": round(t1 - t0, 6),
        "resultado": f"ID {id} eliminado"
    })


@app.post("/sql")
def run_sql(sql: str):
    op = parse_sql(sql)
    if op[0] == "insert":
        isam.add(Registro(op[1]))
        return {"message": f"Insertado {op[1]}"}
    elif op[0] == "search":
        return {"result": [r.val for r in isam.search(op[1])]}
    elif op[0] == "range_search":
        return {"result": [r.val for r in isam.rangeSearch(op[1], op[2])]}
    elif op[0] == "delete":
        isam.remove(op[1])
        return {"message": f"Eliminado {op[1]}"}
    elif op[0] == "load_file_only":
        table_name, file_path = op[1], op[2]
        full_path = os.path.join("data", file_path)

        if not os.path.exists(full_path):
            raise HTTPException(status_code=404, detail=f"Archivo '{file_path}' no encontrado.")

        try:
            df = pd.read_csv(full_path)
            return {"message": f"Archivo '{file_path}' cargado exitosamente con {len(df)} registros.", "columns": df.columns.tolist()}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error al leer el archivo: {e}")

    elif op[0] == "create_from_file":
        table_name, file_path, index_type, index_field = op[1:]

        # Ruta completa al archivo CSV (ajusta si necesario)
        full_path = os.path.join("data", file_path)
        if not os.path.exists(full_path):
            raise HTTPException(status_code=404, detail=f"Archivo {file_path} no encontrado en /data.")

        if index_type != "isam":
            raise HTTPException(status_code=400, detail=f"Índice '{index_type}' no implementado aún.")

        try:
            df = pd.read_csv(full_path)

            if index_field not in df.columns:
                raise HTTPException(status_code=400, detail=f"Campo '{index_field}' no encontrado en el archivo.")

            # Detectar tipo de columna
            col = df[index_field].dropna()

            if pd.api.types.is_numeric_dtype(col):
                registros = [Registro(int(x)) for x in col]
            elif pd.api.types.is_datetime64_any_dtype(pd.to_datetime(col, errors='coerce')):
                registros = [Registro(int(pd.to_datetime(x).timestamp())) for x in col]
            else:
                raise HTTPException(status_code=400, detail=f"La columna '{index_field}' no puede ser indexada como entero.")

            isam.build_index(registros)

            return {"message": f"Índice ISAM creado sobre campo '{index_field}' del archivo '{file_path}'."}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error al procesar archivo: {str(e)}")

    else:
        raise HTTPException(status_code=400, detail="Consulta no válida.")


@app.post("/upload_csv")
async def upload_csv(file: UploadFile = File(...)):
    data_folder = "data"
    os.makedirs(data_folder, exist_ok=True)

    file_path = os.path.join(data_folder, file.filename)
    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)

    return {"message": f"Archivo '{file.filename}' guardado en /data."}


@app.get("/metrics/export")
def export_metrics():
    with open("data/timings.json", "w") as f:
        json.dump(timing_log, f, indent=2)
    return {"message": "Métricas guardadas en data/timings.json"}
