

# Interfaces públicas para conectar con frontend
isam = ISAM("datos_isam.bin", fb=3)

def search_key(key: int):
    return [r.val for r in isam.search(key)]

def search_range(start: int, end: int):
    return [r.val for r in isam.rangeSearch(start, end)]

def add_value(val: int):
    isam.add(Registro(val))

def delete_key(key: int):
    isam.remove(key)
