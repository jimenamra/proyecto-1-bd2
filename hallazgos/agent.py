from typing import Dict, Any
from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from google.adk.tools.agent_tool import AgentTool
import httpx
from datetime import datetime
from backend.indices.rtree import RTreeIndex

# === INSTANCIA DEL R-TREE PARA REGISTRAR CONSULTAS ===
query_rtree = RTreeIndex("data/consultas_hechas.dat")

def next_query_id():
    return int(datetime.now().timestamp())

# === TOOL: Extraer coordenadas explícitas ===

extract_coords_agent = Agent(
    name="extract_coords",
    model="gemini-2.0-flash",
    description="Extrae coordenadas explícitas del texto.",
    instruction="""
Devuelve:
{
  "parsed_coords": { "lat": ..., "lon": ... }
}
Si no hay coordenadas explícitas, devuelve: { "parsed_coords": null }
"""
)
extract_coords_tool = AgentTool(agent=extract_coords_agent)

# === TOOL: Detectar si es noticia o reporte ===

interpretar_tipo_agent = Agent(
    name="interpretar_tipo",
    model="gemini-2.0-flash",
    description="Clasifica si el usuario busca noticias o reportes urbanos.",
    instruction="""
Dado un mensaje, responde con:
{
  "tipo": "noticia" | "reporte",
  "horas": 24
}
"""
)
interpretar_tipo_tool = AgentTool(agent=interpretar_tipo_agent)

# === TOOL: Detectar lugar o ciudad desde lenguaje natural ===

interpretar_lugar_o_ciudad_agent = Agent(
    name="interpretar_lugar_o_ciudad",
    model="gemini-2.0-flash",
    description="Interpreta ciudades/lugares del lenguaje natural y devuelve coordenadas aproximadas.",
    instruction="""
Cuando el mensaje mencione una ciudad o lugar (por ejemplo, "Cercado de Lima", "Arequipa", "Chorrillos"),
devuelve un diccionario como:
{
  "parsed_coords": { "lat": ..., "lon": ... }
}

Ejemplos:
- "¿Qué pasó en Arequipa?" → parsed_coords = { "lat": -16.3988, "lon": -71.5369 }
- "Hay noticias recientes en Chorrillos" → parsed_coords = { "lat": -12.1904, "lon": -77.0230 }
"""
)
interpretar_lugar_tool = AgentTool(agent=interpretar_lugar_o_ciudad_agent)

# === TOOL: Buscar noticias ===

async def buscar_noticias_tool(query: str, tool_context=None) -> Dict[str, Any]:
    coords = getattr(tool_context, "parsed_coords", None)
    horas = getattr(tool_context, "horas", 24)
    if not coords:
        return {"status": "error", "response": "No se detectaron coordenadas para la búsqueda."}
    lat, lon = coords["lat"], coords["lon"]
    query_rtree.add({
        "id": next_query_id(),
        "lat": lat,
        "lon": lon,
        "descripcion": f"búsqueda noticia: {query}"
    })
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(
                f"http://localhost:8000/range?lat_min={lat-0.01}&lon_min={lon-0.01}"
                f"&lat_max={lat+0.01}&lon_max={lon+0.01}&hours={horas}"
            )
        data = res.json()
        if not data:
            return {"status": "success", "response": "No se encontraron noticias recientes en la zona indicada."}
        texto = "\n".join([f"- {x['descripcion']} en ({x['lat']},{x['lon']})" for x in data])
        return {"status": "success", "response": texto}
    except Exception as e:
        return {"status": "error", "response": f"Error al buscar noticias: {str(e)}"}

# === TOOL: Buscar reportes ===

async def buscar_reportes_tool(query: str, tool_context=None) -> Dict[str, Any]:
    coords = getattr(tool_context, "parsed_coords", None)
    horas = getattr(tool_context, "horas", 24)
    if not coords:
        return {"status": "error", "response": "No se detectaron coordenadas para la búsqueda."}
    lat, lon = coords["lat"], coords["lon"]
    query_rtree.add({
        "id": next_query_id(),
        "lat": lat,
        "lon": lon,
        "descripcion": f"búsqueda reporte: {query}"
    })
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(
                f"http://localhost:8000/range?lat_min={lat-0.01}&lon_min={lon-0.01}"
                f"&lat_max={lat+0.01}&lon_max={lon+0.01}&hours={horas}"
            )
        data = res.json()
        if not data:
            return {"status": "success", "response": "No se encontraron reportes recientes en la zona indicada."}
        texto = "\n".join([f"- {x['descripcion']} en ({x['lat']},{x['lon']})" for x in data])
        return {"status": "success", "response": texto}
    except Exception as e:
        return {"status": "error", "response": f"Error al buscar reportes: {str(e)}"}

# === TOOL: Ejecutar SQL directamente ===

async def ejecutar_sql_tool(query: str, tool_context=None) -> Dict[str, Any]:
    try:
        async with httpx.AsyncClient() as client:
            res = await client.post("http://localhost:8000/sql", json={"sql": query})
        return {"status": "success", "response": res.json()}
    except Exception as e:
        return {"status": "error", "response": f"Error al ejecutar SQL: {str(e)}"}

# === ROOT AGENT FINAL ===

root_agent = Agent(
    name="hallazgos_agent",
    model="gemini-2.0-flash",
    description="Agente multimodal para reportes urbanos y noticias recientes georreferenciadas.",
    instruction="""
Eres un agente inteligente capaz de procesar lenguaje natural y entradas multimodales para responder consultas sobre:

- Noticias recientes por ubicación
- Reportes urbanos (baches, cortes, emergencias)
- Búsqueda semántica de ciudades o coordenadas
- Consultas SQL directas

Flujo:
1. Usa `extract_coords` para buscar coordenadas explícitas.
2. Si no encuentra, usa `interpretar_lugar_o_ciudad` para inferir lat/lon por ciudad.
3. Usa `interpretar_tipo` para saber si el usuario quiere "noticia" o "reporte".
4. Llama a `buscar_noticias_tool` o `buscar_reportes_tool` según el tipo.
5. También acepta SQL directamente.

Ejemplos:
- "¿Qué pasó en Arequipa ayer?"
- "¿Hay cortes de agua en Miraflores?"
- "select * from hallazgos where descripcion like '%corte%'"
""",
    tools=[
        extract_coords_tool,
        interpretar_lugar_tool,
        interpretar_tipo_tool,
        FunctionTool(func=buscar_noticias_tool),
        FunctionTool(func=buscar_reportes_tool),
        FunctionTool(func=ejecutar_sql_tool),
    ]
)
