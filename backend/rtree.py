from rtree import index
import json
import os

class RTreeIndex:
    def __init__(self, name="data/rtree_index"):
        self.name = name
        self._delete_existing_index_files()
        self.data_file = f"{name}.json"
        p = index.Property()
        p.dimension = 2
        p.storage = index.RT_Disk
        p.dat_extension = 'dat'
        p.idx_extension = 'idx'
        self.idx = index.Index(name, properties=p, interleaved=True)

        if os.path.exists(self.data_file):
            with open(self.data_file) as f:
                self.data = json.load(f)
        else:
            self.data = {}

    def _delete_existing_index_files(self):
        """Elimina archivos corruptos si ya existen"""
        for ext in [".dat", ".idx",".json"]:
            filepath = f"{self.name}{ext}"
            if os.path.exists(filepath):
                print(f"[INFO] Eliminando archivo corrupto: {filepath}")
                os.remove(filepath)

    def insert(self, record_id: int, lat: float, lon: float, record_data: dict):
        self.idx.insert(record_id, (lon, lat, lon, lat))  # (x_min, y_min, x_max, y_max)
        self.data[str(record_id)] = {
            "lat": lat,
            "lon": lon,
            **record_data
        }
        with open(self.data_file, "w") as f:
            json.dump(self.data, f, indent=2)

    def search_by_id(self, record_id: int):
        return self.data.get(str(record_id), None)

    def rangeSearch(self, lon_min: float, lat_min: float, lon_max: float, lat_max: float):
        results = self.idx.intersection((lon_min, lat_min, lon_max, lat_max))
        return [self.data[str(i)] for i in results if str(i) in self.data]

    def remove(self, record_id: int):
        if str(record_id) not in self.data:
            return
        lat = self.data[str(record_id)]["lat"]
        lon = self.data[str(record_id)]["lon"]
        self.idx.delete(record_id, (lat, lon, lat, lon))
        del self.data[str(record_id)]
        with open(self.data_file, "w") as f:
            json.dump(self.data, f, indent=2)
