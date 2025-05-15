from backend.indices.rtree import RTreeIndex
import os

def test_rtree_index():
    ruta_correcta = os.path.join("backend", "data", "hallazgos_location.dat")
    rtree = RTreeIndex(archivo=ruta_correcta)

    # Leer todos los registros
    registros = rtree.all_records()
    
    print("📦 Registros en RTree:")
    for r in registros:
        print(r)

    #rtree.remove(1)
if __name__ == "__main__":
    test_rtree_index()
