import csv
import os

class SequentialFileIndex:
    def __init__(self, filepath="backend/data/eventos_dataset.csv"):
        self.filepath = filepath
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self.campos = ["id", "nombre", "categoria", "ubicacion"]

        # Si el archivo no existe, lo creamos con cabecera
        if not os.path.exists(self.filepath):
            with open(self.filepath, mode='w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.campos)
                writer.writeheader()

    def agregar(self, registro: dict):
        with open(self.filepath, mode='a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.campos)
            writer.writerow(registro)

    def buscar(self, campo, valor):
        resultados = []
        with open(self.filepath, mode='r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row[campo] == valor:
                    resultados.append(row)
        return resultados

    def listar(self):
        with open(self.filepath, mode='r') as f:
            reader = csv.DictReader(f)
            return list(reader)
