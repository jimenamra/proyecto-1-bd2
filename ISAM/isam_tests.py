import unittest
import os
from isam import ISAM, Registro

class TestISAM(unittest.TestCase):
    TEST_FILE = 'test_isam.dat'
    FB = 3

    def setUp(self):
        # Eliminar archivo de prueba antes de cada ejecución
        if os.path.exists(self.TEST_FILE):
            os.remove(self.TEST_FILE)
        self.isam = ISAM(self.TEST_FILE, self.FB)
        registros = [Registro(5), Registro(3), Registro(9), Registro(4), Registro(8), Registro(1)]
        self.isam.build_index(registros)

    def tearDown(self):
        # Eliminar archivo de prueba después de cada ejecución
        if os.path.exists(self.TEST_FILE):
            os.remove(self.TEST_FILE)

    def test_search(self):
        resultados = self.isam.search(4)
        self.assertEqual(len(resultados), 1)
        self.assertEqual(resultados[0].val, 4)

        resultados = self.isam.search(7)
        self.assertEqual(len(resultados), 0)

    def test_range_search(self):
        resultados = self.isam.rangeSearch(3, 8)
        self.assertEqual(sorted(r.val for r in resultados), [3, 4, 5, 8])

    def test_add(self):
        self.isam.add(Registro(7))
        resultados = self.isam.search(7)
        self.assertEqual(len(resultados), 1)
        self.assertEqual(resultados[0].val, 7)

    def test_remove(self):
        self.isam.remove(5)
        resultados = self.isam.search(5)
        self.assertEqual(len(resultados), 0)

if __name__ == '__main__':
    unittest.main()
