# backend/core/dispatcher.py

from sql_parser import SQLParser
from indices.sequential_file import SequentialFileIndex
from indices.gin import GINIndex
from models import EventoUrbano
from datetime import datetime

class CommandDispatcher:
    def __init__(self):
        self.gin = GINIndex()
        self.seq = SequentialFileIndex()
        self.eventos = {}
        self.sql_parser = SQLParser()
        self.next_id = 1

    def execute(self, command_data):
        cmd = command_data["command"]
        target = command_data.get("target", "eventos")

        if cmd == "AGREGAR":
            if target == "eventos_dataset":
                self.seq.agregar(command_data["contenido"])
                return "✅ Registro agregado al dataset tabulado"
            else:
                evento = EventoUrbano(
                    id=self.next_id,
                    titulo="Evento sin título",
                    descripcion=command_data["descripcion"],
                    categoria="general",
                    fecha=datetime.now(),
                    coordenadas=command_data["coordenadas"],
                    embedding=[]
                )
                self.eventos[self.next_id] = evento
                self.gin.index_document(self.next_id, evento.descripcion)
                self.next_id += 1
                return f"✅ Evento registrado con ID {evento.id}"

        elif cmd == "BUSCAR":
            if target == "eventos_dataset":
                campo = command_data["campo"]
                valor = command_data["valor"]
                resultados = self.seq.buscar(campo, valor)
                return f"🔍 Se encontraron {len(resultados)} coincidencias: {resultados}"
            else:
                palabra = command_data["palabra"]
                ids = self.gin.buscar(palabra)
                eventos = [self.eventos[eid].descripcion for eid in ids]
                return f"🔍 Se encontraron {len(eventos)} evento(s): {eventos}"

        else:
            return "⚠️ Comando no implementado."

    def execute_sql(self, sql_query: str):
        condiciones = self.sql_parser.parse(sql_query)
        if "target" in condiciones and condiciones["target"] == "eventos_dataset":
            campo = condiciones["campo"]
            valor = condiciones["valor"]
            return self.seq.buscar(campo, valor)
        return []
