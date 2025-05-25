import os
import struct
from isam import Registro, Bucket

class BTNode:
    def __init__(self, leaf=False):
        self.leaf = leaf
        self.keys = []
        self.children = []
        self.offsets = []
        self.next = None

class BTree:
    def __init__(self, file_idx='data/bptree_index.dat', file_data='data/bptree_data.dat', t=3, fb=3):
        self.file_idx = file_idx
        self.file_data = file_data
        self.t = t
        self.fb = fb
        self.root = BTNode(leaf=True)

        os.makedirs(os.path.dirname(self.file_idx), exist_ok=True)
        os.makedirs(os.path.dirname(self.file_data), exist_ok=True)
        if not os.path.exists(self.file_idx):
            open(self.file_idx, 'wb').close()
        if not os.path.exists(self.file_data):
            open(self.file_data, 'wb').close()
        
    def _write_data(self, registro):
        with open(self.file_data, 'ab') as f:
            offset = f.tell()
            f.write(registro.empaquetar())
            return offset

    def _read_data(self, offset):
        with open(self.file_data, 'rb') as f:
            f.seek(offset)
            data = f.read(Registro.SIZE)
            return Registro.desempaquetar(data)

    def search(self, key, node):
        if node is None:
            node = self.root
        if node.leaf:
            for i, k in enumerate(node.keys):
                if k == key:
                    return self._read_data(node.offsets[i])
            return None
        else:
            for i, k in enumerate(node.keys):
                if key < k:
                    return self.search(key, node.children[i])
            return self.search(key, node.children[-1])

    def add(self, registro):
        key = registro.val
        root = self.root
        
        if len(root.keys) == (2 * self.t) - 1:
            new_root = BTNode()
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root
            self._add_non_full(self.root, key, registro)
        else:
            self._add_non_full(root, key, registro)
        
        self.save_idx()

    def _add_non_full(self, node, key, registro):
        if node.leaf:
            offset = self._write_data(registro)
            idx = 0
            while idx < len(node.keys) and node.keys[idx] < key:
                idx += 1
            node.keys.insert(idx, key)
            node.offsets.insert(idx, offset)
        else:
            i = len(node.keys) - 1
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            child = node.children[i]
            if len(child.keys) == (2 * self.t) - 1:
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
            self._add_non_full(node.children[i], key, registro)

    def _split_child(self, parent, i):
        t = self.t
        node = parent.children[i]
        new_node = BTNode(leaf=node.leaf)

        if node.leaf:
            new_node.keys = node.keys[t-1:]
            new_node.offsets = node.offsets[t-1:]
            node.keys = node.keys[:t-1]
            node.offsets = node.offsets[:t-1]
            
            new_node.next = node.next
            node.next = new_node
            
            parent.keys.insert(i, new_node.keys[0])
            parent.children.insert(i+1, new_node)
        else:
            parent.keys.insert(i, node.keys[t-1])
            new_node.keys = node.keys[t:]
            node.keys = node.keys[:t-1]
            new_node.children = node.children[t:]
            node.children = node.children[:t]
            parent.children.insert(i+1, new_node)

    def range_search(self, start, end):
        result = []
        node = self.root
        
        while not node.leaf:
            i = 0
            while i < len(node.keys) and start >= node.keys[i]:
                i += 1
            node = node.children[i]
        
        while node is not None:
            for i, k in enumerate(node.keys):
                if start <= k <= end:
                    result.append(self._read_data(node.offsets[i]))
                elif k > end:
                    return result
            node = node.next
        return result

    def remove(self, key):
        node = self.root
        
        while not node.leaf:
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            node = node.children[i]
        
        for i, k in enumerate(node.keys):
            if k == key:
                node.keys.pop(i)
                node.offsets.pop(i)
                break
        self.save_idx()

    def save_idx(self):
        with open(self.file_idx, 'wb') as f:
            leaves = []
            self.getHojas(self.root, leaves)
            for node in leaves:
                for k, offset in zip(node.keys, node.offsets):
                    f.write(struct.pack('ii', k, offset))

    def getHojas(self, node, leaves):
        if node.leaf:
            leaves.append(node)
        else:
            for child in node.children:
                self.getHojas(child, leaves)

    def load_idx(self):
        if not os.path.exists(self.file_idx):
            return
        self.root = BTNode(leaf=True)
        
        with open(self.file_idx, 'rb') as f:
            while True:
                data = f.read(8)
                if not data or len(data) < 8:
                    break
                k, offset = struct.unpack('ii', data)
                self.root.keys.append(k)
                self.root.offsets.append(offset)
