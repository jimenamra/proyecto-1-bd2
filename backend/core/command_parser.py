import re

class CommandParser:
    def parse(self, message: str) -> dict:
        if message.startswith("AGREGAR:"):
            return self._parse_agregar(message)
        elif message.startswith("BUSCAR:"):
            return self._parse_buscar(message)
        else:
            raise ValueError("Instrucción no reconocida")

    def _parse_agregar(self, message):
        # Lógica simplificada con regex
        coords = re.findall(r'-?\d+\.\d+', message)
        return {
            "command": "AGREGAR",
            "descripcion": message,
            "coordenadas": tuple(map(float, coords[:2])) if len(coords) >= 2 else None
        }

    def _parse_buscar(self, message):
        coords = re.findall(r'-?\d+\.\d+', message)
        return {
            "command": "BUSCAR",
            "coordenadas": tuple(map(float, coords[:2])) if len(coords) >= 2 else None,
            "radio": 500  # puedes extraer radio también si lo deseas
        }
