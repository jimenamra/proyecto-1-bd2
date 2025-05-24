import struct
import os
from datetime import datetime, timedelta
from rtree import index

class RTreeIndex:
    def __init__(self, archivo="data/hallazgos_location.dat"):
        os.makedirs(os.path.dirname(archivo), exist_ok=True)
        self.archivo = archivo
        self.struct_fmt = "iff50s26s"  # id, lat, lon, descripcion, timestamp
        self.struct_size = struct.calcsize(self.struct_fmt)
        self.idx = index.Index()
        self._cargar_registros()

    def _cargar_registros(self):
        """Carga todos los registros del archivo físico al índice RTree"""
        if not os.path.exists(self.archivo):
            return
        with open(self.archivo, "rb") as f:
            while chunk := f.read(self.struct_size):
                id_, lat, lon, desc_bytes, ts_bytes = struct.unpack(self.struct_fmt, chunk)
                desc = desc_bytes.decode("utf-8").strip('\x00')
                ts = ts_bytes.decode("utf-8").strip('\x00')
                self.idx.insert(id_, (lat, lon, lat, lon), obj={
                    "id": id_, "lat": lat, "lon": lon, "descripcion": desc, "timestamp": ts
                })

    def add(self, record):
        """Agrega un nuevo registro al archivo físico y al índice"""
        id_, lat, lon, desc = record['id'], record['lat'], record['lon'], record['descripcion']
        ts = datetime.utcnow().isoformat(timespec='seconds')

        desc_bytes = desc.encode('utf-8')[:50].ljust(50, b'\x00')
        ts_bytes = ts.encode('utf-8')[:26].ljust(26, b'\x00')

        packed = struct.pack(self.struct_fmt, id_, lat, lon, desc_bytes, ts_bytes)

        with open(self.archivo, "ab") as f:
            f.write(packed)

        self.idx.insert(id_, (lat, lon, lat, lon), obj={
            "id": id_, "lat": lat, "lon": lon, "descripcion": desc, "timestamp": ts
        })

    def range_search_recent(self, lat_min, lon_min, lat_max, lon_max, max_hours=24):
        """Busca todos los registros en el área indicada y que sean recientes (últimas X horas)"""
        cutoff = datetime.utcnow() - timedelta(hours=max_hours)
        box = (lat_min, lon_min, lat_max, lon_max)
        results = []
        for r in self.idx.intersection(box, objects=True):
            ts = datetime.fromisoformat(r.object["timestamp"])
            if ts >= cutoff:
                results.append(r.object)
        return results

    def remove(self, id_):
        """Elimina un registro física y lógicamente (reconstruye archivo e índice sin ese ID)"""
        nuevos_registros = []
        eliminado = False

        with open(self.archivo, "rb") as f:
            while chunk := f.read(self.struct_size):
                rid, lat, lon, desc_bytes, ts_bytes = struct.unpack(self.struct_fmt, chunk)
                if rid != id_:
                    nuevos_registros.append((rid, lat, lon, desc_bytes, ts_bytes))
                else:
                    eliminado = True

        if eliminado:
            # Sobreescribir archivo y reconstruir índice
            with open(self.archivo, "wb") as f:
                for r in nuevos_registros:
                    f.write(struct.pack(self.struct_fmt, *r))

            self.idx = index.Index()
            for r in nuevos_registros:
                rid, lat, lon, desc_bytes, ts_bytes = r
                desc = desc_bytes.decode("utf-8").strip('\x00')
                ts = ts_bytes.decode("utf-8").strip('\x00')
                self.idx.insert(rid, (lat, lon, lat, lon), obj={
                    "id": rid, "lat": lat, "lon": lon, "descripcion": desc, "timestamp": ts
                })

        return eliminado

    def all_records(self):
        """Devuelve todos los objetos actualmente en el índice"""
        if self.idx.bounds:
            return [r.object for r in self.idx.intersection(self.idx.bounds, objects=True)]
        else:
            return []
