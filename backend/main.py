from fastapi import FastAPI, Query, UploadFile, File, HTTPException
from ISAM.isam import Registro,ISAM
from ISAM.btree import BTree
from .sql_parser import parse_sql
from pydantic import BaseModel
import pandas as pd
import os

app = FastAPI()
isam = ISAM()
btree = BTree()

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
    return {"result": isam.search(key)}

@app.get("/range/{a}/{b}")
def range_search(a: int, b: int):
    return {"result": isam.rangeSearch(a, b)}

@app.post("/insert")
def insert(payload: dict):
    #reg = Registro(data.value)
    #isam.add(reg)
    #return {"message": f"Insertado {data.value}"}
    try:
        r = Registro(
            payload["id"],
            payload["fecha"],
            payload["tipo"],
            float(payload["lat"]),
            float(payload["lon"]),
            float(payload["mag"]),
            float(payload["prof"])
        )

        isam.add(r)
        #btree.add(r)
        #rtree.insert(r.lon, r.lat, r)  # usando lon/lat como bounding box

        return {"message": f"Desastre '{r.id}' registrado."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.delete("/delete/{key}")
def delete(key: int):
    isam.remove(key)
    return {"message": f"Eliminado {key}"}

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
