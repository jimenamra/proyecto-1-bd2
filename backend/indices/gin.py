import re
from collections import defaultdict

class GINIndex:
    def __init__(self):
        # Diccionario invertido: palabra -> set de IDs de eventos
        self.index = defaultdict(set)

    def _tokenizar(self, texto):
        """
        Convierte un texto en una lista de palabras clave.
        Se eliminan signos de puntuación y se convierte a minúsculas.
        """
        texto = texto.lower()
        tokens = re.findall(r'\b\w+\b', texto)
        return tokens

    def index_document(self, doc_id, texto):
        """
        Indexa un documento (descripción del evento) por sus palabras clave.
        """
        tokens = self._tokenizar(texto)
        for token in tokens:
            self.index[token].add(doc_id)

    def buscar(self, palabra):
        """
        Busca eventos que contengan una palabra clave.
        """
        palabra = palabra.lower()
        return list(self.index.get(palabra, []))

    def eliminar(self, doc_id):
        """
        Elimina un documento del índice (por ejemplo, si se borra un evento).
        """
        for palabra in list(self.index.keys()):
            if doc_id in self.index[palabra]:
                self.index[palabra].remove(doc_id)
                if not self.index[palabra]:  # Limpieza si ya no hay IDs
                    del self.index[palabra]

    def listar_indice(self):
        """
        Retorna el contenido del índice invertido (para debugging).
        """
        return dict(self.index)
