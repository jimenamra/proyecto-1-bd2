import os
import struct
from typing import TypeVar, Generic, List, Optional

TK = TypeVar('TK')
RecordType = TypeVar('RecordType')

MAX_DEPTH = 6  # Profundidad máxima del directorio
MAX_FILL = 10  # Capacidad máxima de cada bucket


class ExtendibleHashing(Generic[TK, RecordType]):
    def __init__(self, filename: str, record_struct: struct.Struct):
        self.index_filename = f"indx_{filename}"
        self.data_filename = filename
        self.record_struct = record_struct

        # Crear archivo de datos si no existe
        if not os.path.exists(self.data_filename):
            with open(self.data_filename, 'wb') as _:
                pass

        # Crear archivo de índice si no existe
        if not os.path.exists(self.index_filename):
            with open(self.index_filename, 'wb') as f:
                for i in range(1 << MAX_DEPTH):
                    f.write(struct.pack("ii", i, i % 2))

    def hash_function(self, key: int) -> int:
        return key % (1 << MAX_DEPTH)

    def get_bucket_index(self, key: int) -> int:
        index = self.hash_function(key)
        with open(self.index_filename, 'rb') as f:
            f.seek(index * 8 + 4)  # 4 bytes skip hash index
            bucket_index = struct.unpack("i", f.read(4))[0]
        return bucket_index

    def insert(self, record):
        if self.find(record.id) is None:
            with open(self.data_filename, 'ab') as f:
                f.write(self.record_struct.pack(*record.to_tuple()))
            print(f"Registro insertado: {record.id}")
        else:
            print(f"El registro con código {record.id} ya existe.")

    def find(self, key: int) -> Optional[RecordType]:
        with open(self.data_filename, 'rb') as f:
            while chunk := f.read(self.record_struct.size):
                data = self.record_struct.unpack(chunk)
                if data[0] == key:
                    return type('DynamicRecord', (), {'id': data[0], 'to_tuple': lambda self: data})()
        return None

    def remove(self, key: int) -> bool:
        found = False
        with open(self.data_filename, 'r+b') as f:
            pos = 0
            while chunk := f.read(self.record_struct.size):
                data = self.record_struct.unpack(chunk)
                if data[0] == key:
                    f.seek(pos)
                    f.write(self.record_struct.pack(*([0] * len(data))))
                    found = True
                    print(f"Registro con código {key} eliminado.")
                    break
                pos += self.record_struct.size
        if not found:
            print(f"Registro con código {key} no encontrado.")
        return found

    def find_range(self, lower: int, upper: int) -> List[RecordType]:
        results = []
        with open(self.data_filename, 'rb') as f:
            while chunk := f.read(self.record_struct.size):
                data = self.record_struct.unpack(chunk)
                if lower <= data[0] <= upper:
                    results.append(type('DynamicRecord', (), {'id': data[0], 'to_tuple': lambda self: data})())
        return results

    def get_all_records(self) -> List[RecordType]:
        records = []
        with open(self.data_filename, 'rb') as f:
            while chunk := f.read(self.record_struct.size):
                data = self.record_struct.unpack(chunk)
                records.append(type('DynamicRecord', (), {'id': data[0], 'to_tuple': lambda self: data})())
        return records
