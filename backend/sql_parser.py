import re

class SQLParser:
    def parse(self, query: str) -> dict:
        """
        Soporta consultas del tipo:
        SELECT * FROM eventos WHERE campo1 = 'valor' AND campo2 BETWEEN x AND y ...
        """
        query = query.strip().lower()

        if not query.startswith("select"):
            raise ValueError("Solo se soportan consultas SELECT.")

        condiciones = {}
        # Extrae condiciones tipo campo = 'valor'
        matches = re.findall(r"(\w+)\s*=\s*'([^']*)'", query)
        for campo, valor in matches:
            condiciones[campo] = valor

        # Extrae rangos tipo lat BETWEEN a AND b
        rangos = re.findall(r"(\w+)\s+between\s+(-?\d+\.?\d*)\s+and\s+(-?\d+\.?\d*)", query)
        for campo, val1, val2 in rangos:
            condiciones[campo + "_between"] = (float(val1), float(val2))

        return condiciones
