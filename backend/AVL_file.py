import struct
import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class AVLRecord:
    id: int
    left: int
    right: int
    height: int
    offset: int

class AVLFile:
    def __init__(self, filename: str):
        self.filename = filename

        self.record_struct = struct.Struct("iiiii")
        self.registro_struct = struct.Struct("i10s10sffff")  # para Registro real
        self.pos_root_struct = struct.Struct("i")

        if not os.path.exists(self.filename):
            with open(self.filename, "wb") as file:
                file.write(self.pos_root_struct.pack(-1))  # raíz vacía

        with open(self.filename, "rb") as file:
            self.pos_root = self.pos_root_struct.unpack(file.read(4))[0]

    def update_height(self, record: AVLRecord):
        left_height = self.get_record(record.left).height if record.left != -1 else -1
        right_height = self.get_record(record.right).height if record.right != -1 else -1
        record.height = max(left_height, right_height) + 1

    def get_balance_factor(self, record: AVLRecord) -> int:
        left_height = self.get_record(record.left).height if record.left != -1 else -1
        right_height = self.get_record(record.right).height if record.right != -1 else -1
        return left_height - right_height

    def set_record(self, pos: int, record: AVLRecord):
        with open(self.filename, "r+b") as file:
            file.seek(4 + pos * self.record_struct.size)
            file.write(self.record_struct.pack(record.id, record.left, record.right, record.height, record.offset))

    def get_record(self, pos: int) -> AVLRecord:
        with open(self.filename, "rb") as file:
            file.seek(4 + pos * self.record_struct.size)
            data = file.read(self.record_struct.size)
            return AVLRecord(*self.record_struct.unpack(data))

    def append_record(self, record: AVLRecord) -> int:
        with open(self.filename, "ab") as file:
            file.write(self.record_struct.pack(record.id, record.left, record.right, record.height, record.offset))
        return self.get_num_records() - 1

    def get_num_records(self) -> int:
        size = os.path.getsize(self.filename)
        return (size - 4) // self.record_struct.size

    def _save_registro(self, registro) -> int:
        packed = self.registro_struct.pack(
            registro.id,
            registro.fecha.encode('utf-8').ljust(10, b'\x00'),
            registro.tipo.encode('utf-8').ljust(10, b'\x00'),
            registro.lat,
            registro.lon,
            registro.mag,
            registro.prof
        )
        with open(self.filename, "ab") as f:
            offset = f.tell()
            f.write(packed)
        return offset

    def save_root(self):
        with open(self.filename, "r+b") as file:
            file.seek(0)
            file.write(self.pos_root_struct.pack(self.pos_root))

    def insert(self, record):
        offset = self._save_registro(record)
        record = AVLRecord(record.id, -1, -1, 0, offset)
        self.pos_root = self._insert(self.pos_root, record)
        self.save_root()

    def _insert(self, pos: int, record: AVLRecord) -> int:
        if pos == -1:
            return self.append_record(record)

        temp = self.get_record(pos)

        if record.id < temp.id:
            temp.left = self._insert(temp.left, record)
        elif record.id > temp.id:
            temp.right = self._insert(temp.right, record)
        else:
            return pos  # id ya existe

        self.update_height(temp)
        self.set_record(pos, temp)

        return self.balance(pos)

    def remove(self, id: int):
        self.pos_root = self._remove(self.pos_root, id)
        self.save_root()

    def _remove(self, pos: int, id: int) -> int:
        if pos == -1:
            return -1

        record = self.get_record(pos)

        if id < record.id:
            record.left = self._remove(record.left, id)
        elif id > record.id:
            record.right = self._remove(record.right, id)
        else:
            if record.left == -1:
                return record.right
            elif record.right == -1:
                return record.left

            min_pos = self.get_min(record.right)
            min_record = self.get_record(min_pos)
            record.id = min_record.id
            record.right = self._remove(record.right, min_record.id)

        self.update_height(record)
        self.set_record(pos, record)
        return self.balance(pos)

    def balance(self, pos: int) -> int:
        record = self.get_record(pos)
        balance = self.get_balance_factor(record)

        if balance > 1:
            left = self.get_record(record.left)
            if self.get_balance_factor(left) < 0:
                record.left = self.rotate_left(record.left)
                self.set_record(pos, record)
            return self.rotate_right(pos)

        if balance < -1:
            right = self.get_record(record.right)
            if self.get_balance_factor(right) > 0:
                record.right = self.rotate_right(record.right)
                self.set_record(pos, record)
            return self.rotate_left(pos)

        return pos

    def rotate_left(self, pos: int) -> int:
        x = self.get_record(pos)
        y_pos = x.right
        y = self.get_record(y_pos)

        x.right = y.left
        y.left = pos

        self.update_height(x)
        self.update_height(y)

        self.set_record(pos, x)
        self.set_record(y_pos, y)

        return y_pos

    def rotate_right(self, pos: int) -> int:
        x = self.get_record(pos)
        y_pos = x.left
        y = self.get_record(y_pos)

        x.left = y.right
        y.right = pos

        self.update_height(x)
        self.update_height(y)

        self.set_record(pos, x)
        self.set_record(y_pos, y)

        return y_pos

    def get_min(self, pos: int) -> int:
        current_pos = pos
        current = self.get_record(current_pos)
        while current.left != -1:
            current_pos = current.left
            current = self.get_record(current_pos)
        return current_pos

    def inorder(self, pos: Optional[int] = None, result: Optional[list] = None):
        if result is None:
            result = []
        if pos is None:
            pos = self.pos_root
        if pos == -1:
            return result
        record = self.get_record(pos)
        self.inorder(record.left, result)
        result.append(record.id)
        self.inorder(record.right, result)
        return result


    def search(self, key: int) -> Optional[dict]:
        return self._search(self.pos_root, key)

    def _search(self, pos: int, key: int) -> Optional[dict]:
        if pos == -1:
            return None
        r = self.get_record(pos)
        if key == r.id:
            return self._read_registro(r.offset)
        elif key < r.id:
            return self._search(r.left, key)
        else:
            return self._search(r.right, key)

    def _read_registro(self, offset) -> dict: # para el offset
        with open(self.filename, "rb") as f:
            f.seek(offset)
            data = f.read(self.registro_struct.size)
            unpacked = self.registro_struct.unpack(data)
            return {
                "id": unpacked[0],
                "fecha": unpacked[1].decode().strip("\x00"),
                "tipo": unpacked[2].decode().strip("\x00"),
                "lat": unpacked[3],
                "lon": unpacked[4],
                "mag": unpacked[5],
                "prof": unpacked[6]
            }
