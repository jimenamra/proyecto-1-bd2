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
from .Extendible_Hashing import ExtendibleHashing
from .AVL_file import AVLFile   
import struct
import csv

app = FastAPI()
isam = ISAM()
btree = BTree()
avl = AVLFile("data/avl_index.dat") 
rtree = RTreeIndex()
record_struct = struct.Struct('i10s10sffff')
ext_hash = ExtendibleHashing("ext_hash.dat", record_struct)

# estado para logica de parser
tabla_activa = None
indice_activo = None
columna_indice = None
tipo_columna = None  # int/float

timing_log = []  # metricas

class RegistroRequest(BaseModel):
    id: int
    fecha: str
    tipo: str
    lat: float
    lon: float
    mag: float
    prof: float


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

    t0 = time()
    avl_result = avl.search(key)
    t1 = time()
    response.append({
        "metodo": "AVL",
        "tiempo": round(t1 - t0, 6),
        "resultado": avl_result if avl_result else "No encontrado"
    })

    t0 = time()
    r = ext_hash.find(key)
    t1 = time()
    response.append({
        "metodo": "Hashing",
        "tiempo": round(t1 - t0, 6),
        "resultados": r.to_tuple() if r else []
    })

    # # RTree (comentado)
    t0 = time()
    rtree_result = rtree.search_by_id(key)
    t1 = time()
    response.append({
        "metodo": "RTree",
        "tiempo": round(t1 - t0, 6),
        "resultados": rtree_result
    })

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
        "resultados": btree_result
    })

    t0 = time()
    avl_result = avl.range_search(a, b)
    t1 = time()
    response.append({
        "metodo": "AVL",
        "tiempo": round(t1 - t0, 6),
        "resultados": avl_result
    })
    return response

@app.get("/range_area/{a}/{b}")
def range_search_area(a: float, b: float):
    
    if not (-90 <= a <= 90 and -180 <= b <= 180):
        raise HTTPException(status_code=400, detail="Coordenadas fuera de rango")

    # RTree 
    response = []
    t0 = time()
    rtree_result = rtree.rangeSearch(a, b)
    t1 = time()
    response.append({
        "metodo": "RTree",
        "tiempo": round(t1 - t0, 6),
        "resultados": rtree_result
    })

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

    t0 = time()
    avl.insert(r)  
    t1 = time()
    response.append({
        "metodo": "AVL",
        "tiempo": round(t1 - t0, 6),
        "resultado": f"ID {r.id} insertado"
    })

    t0 = time()
    ext_hash.insert(r)
    t1 = time()
    response.append({
        "metodo": "Hashing",
        "tiempo": round(t1 - t0, 6),
        "resultado": f"ID {r.id} insertado"
    })

    # # RTree (comentado)
    t0 = time()
    rtree.insert(
        record_id=r.id,
        lat=r.lat,
        lon=r.lon,
        record_data={
            "fecha": r.fecha,
            "tipo": r.tipo,
            "mag": r.mag,
            "prof": r.prof
        }
    )
    t1 = time()
    response.append({
        "metodo": "RTree",
        "tiempo": round(t1 - t0, 6),
        "resultado": f"ID {r.id} insertado"
    })

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


    t0 = time()
    avl.remove(id) 
    t1 = time()
    response.append({
        "metodo": "AVL",
        "tiempo": round(t1 - t0, 6),
        "resultado": f"ID {id} eliminado"
    })

    t0 = time()
    ext_hash.remove(id)
    t1 = time()
    response.append({
        "metodo": "Hashing",
        "tiempo": round(t1 - t0, 6),
        "resultado": f"ID {id} eliminado"
    })

    t0 = time()
    rtree.remove(id)
    t1 = time()
    response.append({
        "metodo": "RTree",
        "tiempo": round(t1 - t0, 6),
        "resultado": f"ID {id} eliminado"
    })


    return response


@app.post("/sql")
def run_sql(sql: str = Query(...)):
    global tabla_activa, indice_activo, columna_indice, tipo_columna

    op = parse_sql(sql)

    #CREATE TABLE 
    if op[0] == "create":
        tabla_activa = op[1]
        campos = op[2]

        indexed_fields = [f for f in campos if "index" in f]
        if not indexed_fields:
            raise HTTPException(400, detail="Debe definir al menos un campo con INDEX.")

        campo_idx = indexed_fields[0]
        columna_indice = campo_idx["name"]
        indice_activo = campo_idx["index"].lower()
        tipo_columna = campo_idx["type"].lower()

        if indice_activo not in ["isam", "btree", "avl", "hash", "rtree", "seq"]:
            raise HTTPException(400, detail=f"Índice no válido: {indice_activo}")

        return {
            "message": f"Tabla '{tabla_activa}' creada.",
            "estructura": campos,
            "indexado_por": columna_indice,
            "tipo_indice": indice_activo,
            "tipo_columna": tipo_columna
        }

    # --- validación obligatoria ---
    if not tabla_activa:
        raise HTTPException(400, detail="Debe ejecutar CREATE TABLE primero.")

    # --- SEARCH ---
    if op[0] == "search":
        campo, valor = op[1], op[2]
        if campo != columna_indice:
            raise HTTPException(400, detail=f"Consulta solo válida para '{columna_indice}'")

        if indice_activo == "isam":
            return {"result": [r.__dict__ for r in isam.search(int(valor))]}
        elif indice_activo == "btree":
            r = btree.search(int(valor))
            return {"result": r.__dict__ if r else []}
        elif indice_activo == "avl":
            return {"result": avl.search(int(valor))}
        elif indice_activo == "hash":
            r = ext_hash.find(int(valor))
            return {"result": r.to_tuple() if r else []}
        elif indice_activo == "rtree":
            return {"result": rtree.search_by_id(int(valor))}

    # --- RANGE ---
    if op[0] == "range":
        campo, a, b = op[1], op[2], op[3]
        if campo != columna_indice:
            raise HTTPException(400, detail=f"Consulta solo válida para '{columna_indice}'")

        if indice_activo == "isam":
            return {"result": [r.__dict__ for r in isam.rangeSearch(int(a), int(b))]}
        elif indice_activo == "btree":
            return {"result": [r.__dict__ for r in btree.range_search(int(a), int(b))]}
        elif indice_activo == "hash":
            return {"result": [r.to_tuple() for r in ext_hash.find_range(int(a), int(b))]}
        elif indice_activo == "avl":
            return {"result": avl.range_search(int(a), int(b))}
        elif indice_activo == "rtree":
            return {"result": rtree.rangeSearch(a, b)}

    # --- INSERT ---
    if op[0] == "insert":
        campos = op[1]
        r = Registro(
            int(campos[0]),
            campos[1],
            campos[2],
            float(campos[3]),
            float(campos[4]),
            float(campos[5]),
            float(campos[6])
        )
        if indice_activo == "isam":
            isam.add(r)
        elif indice_activo == "btree":
            btree.add(r)
        elif indice_activo == "avl":
            avl.insert(r)
        elif indice_activo == "hash":
            ext_hash.insert(r)
        elif indice_activo == "rtree":
            rtree.insert(
                record_id=r.id,
                lat=r.lat,
                lon=r.lon,
                record_data={
                    "fecha": r.fecha,
                    "tipo": r.tipo,
                    "mag": r.mag,
                    "prof": r.prof
                }
            )
        return {"message": f"Insertado ID {r.id}"}

    # --- DELETE ---
    if op[0] == "delete":
        campo, valor = op[1], op[2]
        if campo != columna_indice:
            raise HTTPException(400, detail=f"Consulta solo válida para '{columna_indice}'")
        if indice_activo == "isam":
            isam.remove(int(valor))
        elif indice_activo == "btree":
            btree.remove(int(valor))
        elif indice_activo == "avl":
            avl.remove(int(valor))
        elif indice_activo == "hash":
            ext_hash.remove(int(valor))
        elif indice_activo == "rtree":
            rtree.remove(int(valor))
        return {"message": f"Eliminado ID {int(valor)}"}

    raise HTTPException(400, detail="Sentencia no válida o incompleta.")

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
