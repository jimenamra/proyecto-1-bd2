import os
import struct

class Registro:
    FORMAT = 'i10s10sffff'  # id, fecha, tipo, lat, lon, mag, prof
    SIZE = struct.calcsize(FORMAT)

    def __init__(self, id, fecha, tipo, lat, lon, mag, prof):
        self.id = id
        self.fecha = fecha
        self.tipo = tipo
        self.lat = lat
        self.lon = lon
        self.mag = mag
        self.prof = prof

    def empaquetar(self):
        return struct.pack(
            self.FORMAT,
            self.id,
            self.fecha.encode('utf-8'),
            self.tipo.encode('utf-8'),
            self.lat,
            self.lon,
            self.mag,
            self.prof
        )

    @staticmethod
    def desempaquetar(data):
        unpacked = struct.unpack(Registro.FORMAT, data)
        return Registro(
            unpacked[0],
            unpacked[1].decode('utf-8').strip('\x00'),
            unpacked[2].decode('utf-8').strip('\x00'),
            unpacked[3],
            unpacked[4],
            unpacked[5],
            unpacked[6]
        )

    def to_tuple(self):
        return (
            self.id,
            self.fecha.encode('utf-8'),
            self.tipo.encode('utf-8'),
            self.lat,
            self.lon,
            self.mag,
            self.prof
        )


class Bucket:
    def __init__(self, fb):
        self.registros = [None] * fb
        self.size = 0
        self.next = -1
        self.fb = fb

    def insert(self, registro):
        if not self.isFull():
            self.registros[self.size] = registro
            self.size += 1
        else:
            raise Exception("Bucket is full")

    def isFull(self):
        return self.size >= self.fb

    def empaquetar(self):
        data = struct.pack('i', self.size)
        data += struct.pack('i', self.next)
        registro_vacio = struct.pack(
            Registro.FORMAT,
            -1, b'', b'', -1.0, -1.0, -1.0, -1.0
        )
        for i in range(self.fb):
            if i < self.size and self.registros[i]:
                data += self.registros[i].empaquetar()
            else:
                data += registro_vacio
        return data

    @staticmethod
    def desempaquetar(data, fb):
        size, next = struct.unpack('ii', data[:8])
        bucket = Bucket(fb)
        bucket.size = size
        bucket.next = next
        offset = 8
        for i in range(fb):
            registro_data = data[offset:offset + Registro.SIZE]
            r = Registro.desempaquetar(registro_data)
            if i < size:
                bucket.registros[i] = r
            offset += Registro.SIZE
        return bucket

class ISAM:
    def __init__(self, path='data/registros_isam.dat', fb=3): # por default
        self.path = path
        self.fb = fb
        self.index1 = []  # [(clave, offset)]
        self.index2 = []  # [(clave, pos en index1)]
        self.bucket_size = 8 + fb * Registro.SIZE
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            open(path, 'wb').close()

    def _read_bucket(self, offset):
        with open(self.path, 'rb') as f:
            f.seek(offset)
            data = f.read(self.bucket_size)
            return Bucket.desempaquetar(data, self.fb)

    def _write_bucket(self, bucket, offset):
        with open(self.path, 'r+b') as f:
            f.seek(offset)
            f.write(bucket.empaquetar())

    def _append_bucket(self, bucket):
        with open(self.path, 'ab') as f:
            offset = f.tell()
            f.write(bucket.empaquetar())
            return offset

    def build_index(self, registros):
        registros.sort(key=lambda r: r.id)
        self.index1.clear()
        self.index2.clear()
        open(self.path, 'wb').close()
        for i in range(0, len(registros), self.fb):
            bloque = Bucket(self.fb)
            for r in registros[i:i + self.fb]:
                bloque.insert(r)
            offset = self._append_bucket(bloque)
            self.index1.append((bloque.registros[0].id, offset))
        for i in range(0, len(self.index1), 2):
            self.index2.append((self.index1[i][0], i))

    def _buscar_offset(self, key):
        i1 = 0
        for k2, idx in self.index2:
            if key >= k2:
                i1 = idx
        offset = self.index1[i1][1]
        for j in range(i1, len(self.index1)):
            if key >= self.index1[j][0]:
                offset = self.index1[j][1]
            else:
                break
        return offset

    def search(self, key):
        offset = self._buscar_offset(key)
        res = []
        while offset != -1:
            bucket = self._read_bucket(offset)
            for r in bucket.registros:
                if r and r.id == key:
                    res.append(r)
            offset = bucket.next
        return res
    
    def rangeSearch(self, begin, end):
        resultados = []

        # Buscar el índice de nivel 1 desde el cual comenzar
        start_idx = 0
        for i, (k, _) in enumerate(self.index1):
            if k <= begin:
                start_idx = i
            else:
                break

        # Recorrer los buckets desde ese índice hasta que se supere 'end'
        for i in range(start_idx, len(self.index1)):
            _, offset = self.index1[i]
            while offset != -1:
                bucket = self._read_bucket(offset)
                for r in bucket.registros:
                    if r and begin <= r.id <= end:
                        resultados.append(r)
                    elif r and r.id > end:
                        return resultados  # Ya pasamos el rango
                offset = bucket.next

        return resultados

    def add(self, registro):
        if not self.index1:
            # crea un bucket inicial con el registro
            bucket = Bucket(self.fb)
            bucket.insert(registro)
            offset = self._append_bucket(bucket)
            self.index1.append((registro.id, offset))
            self.index2.append((registro.id, 0))
            return
        offset = self._buscar_offset(registro.id)
        bucket = self._read_bucket(offset)
        while bucket.isFull():
            if bucket.next == -1:
                new_bucket = Bucket(self.fb)
                new_offset = self._append_bucket(new_bucket)
                bucket.next = new_offset
                self._write_bucket(bucket, offset)
            offset = bucket.next
            bucket = self._read_bucket(offset)
        bucket.insert(registro)
        self._write_bucket(bucket, offset)

    def remove(self, key):
        if not self.index1:
            return False

        offset = self._buscar_offset(key)
        eliminado = False

        while offset != -1:
            bucket = self._read_bucket(offset)
            modificado = False

            for i in range(bucket.size):
                if bucket.registros[i] and bucket.registros[i].id == key:
                    bucket.registros[i] = None
                    modificado = True
                    eliminado = True

            if modificado:
                self._write_bucket(bucket, offset)

            offset = bucket.next

        return eliminado
