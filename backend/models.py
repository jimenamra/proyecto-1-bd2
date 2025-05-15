from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

@dataclass
class EventoUrbano:
    id: int
    titulo: str
    descripcion: str
    categoria: str
    fecha: datetime
    coordenadas: Tuple[float, float]
    embedding: List[float]  # opcional, si usas IVF
