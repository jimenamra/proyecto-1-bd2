from backend.indices.gin import GINIndex

def test_gin_index():
    gin = GINIndex()

    # Indexar descripciones
    gin.index_document(1, "Feria de comida en el parque Kennedy")
    # gin.index_document(2, "Marcha por el cambio climático en el centro")

    print("🔍 Buscar 'comida':", gin.buscar("parque"))        # Esperado: [1]
    print("🔍 Buscar 'centro':", gin.buscar("centro"))        # Esperado: [2]

    # Eliminar evento 2 y buscar de nuevo
    # gin.eliminar(2)
    # print("🔍 Buscar 'centro' después de eliminar:", gin.buscar("centro"))  # Esperado: []

    print("🧾 Contenido actual del índice:", gin.listar_indice())

if __name__ == "__main__":
    test_gin_index()
