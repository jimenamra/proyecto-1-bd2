import os
import unittest
from isam import Registro
from btree import BTree

class TestBTree(unittest.TestCase):

    def setUp(self):
        self.index_path = 'test_data/bptree_index_test.dat'
        self.data_path = 'test_data/bptree_data_test.dat'

        # Elimina archivos antiguos si existen
        if os.path.exists(self.index_path):
            os.remove(self.index_path)
        if os.path.exists(self.data_path):
            os.remove(self.data_path)

        # Crea una instancia del BTree
        self.tree = BTree(file_idx=self.index_path, file_data=self.data_path, t=3)

    def test_insert_and_search(self):
        values = [10, 20, 30, 40, 50]
        for v in values:
            self.tree.add(Registro(val=v))  # <-- usa add en vez de insert

        for v in values:
            r = self.tree.search(v, None)  # <-- incluye el segundo parámetro
            self.assertIsNotNone(r)
            self.assertEqual(r.val, v)

        # Clave inexistente
        self.assertIsNone(self.tree.search(999, None))

    def test_range_search(self):
        values = [5, 10, 15, 20, 25, 30]
        for v in values:
            self.tree.add(Registro(val=v))  # <-- usa add

        result = self.tree.range_search(12, 27)
        result_values = [r.val for r in result]
        self.assertEqual(result_values, [15, 20, 25])

    def test_remove(self):
        values = [100, 200, 300]
        for v in values:
            self.tree.add(Registro(val=v))  # <-- usa add

        self.tree.remove(200)
        self.assertIsNone(self.tree.search(200, None))
        self.assertIsNotNone(self.tree.search(100, None))
        self.assertIsNotNone(self.tree.search(300, None))

    def tearDown(self):
        if os.path.exists(self.index_path):
            os.remove(self.index_path)
        if os.path.exists(self.data_path):
            os.remove(self.data_path)

if __name__ == '__main__':
    unittest.main()
