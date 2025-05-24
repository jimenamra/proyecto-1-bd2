from ISAM.isam import ISAM

def execute_sql(sql: str):
    isam = ISAM()
    sql = sql.strip().lower()

    if sql.startswith("insert into"):
        num = int(sql.split("values")[1].strip(" ();"))
        isam.insert(num)
        return f"Insertado {num}"

    elif sql.startswith("select * from"):
        if "where" in sql:
            cond = sql.split("where")[1].strip()
            if "between" in cond:
                a, b = map(int, cond.replace("key", "").replace("between", "").split("and"))
                return isam.range_search(a, b)
            else:
                k = int(cond.split("=")[1])
                return isam.search(k)
        else:
            return "Consulta inválida: falta condición WHERE"

    elif sql.startswith("delete from"):
        k = int(sql.split("where")[1].split("=")[1])
        isam.delete(k)
        return f"Eliminado {k}"

    else:
        return "SQL inválido"
