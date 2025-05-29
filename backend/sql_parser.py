import re

def parse_sql(sql: str):
    sql = sql.strip().rstrip(';')

    # --- NUEVO: RANGO ESPACIAL 2D PARA RTREE ---
    match = re.search(
        r'is_in_range2d\s*\(\s*lat_min\s*=>\s*([-\d\.]+)\s*,\s*lat_max\s*=>\s*([-\d\.]+)\s*,\s*lon_min\s*=>\s*([-\d\.]+)\s*,\s*lon_max\s*=>\s*([-\d\.]+)\s*\)',
        sql,
        re.IGNORECASE
    )
    if match:
        lat_min = float(match.group(1))
        lat_max = float(match.group(2))
        lon_min = float(match.group(3))
        lon_max = float(match.group(4))
        return ("range2d", lon_min, lat_min, lon_max, lat_max)

    # --- NUEVO: CREATE TABLE FROM "archivo.csv" USING index("columna") ---
    match = re.match(
        r'create table\s+(\w+)\s+from\s+"(.+?)"\s+using\s+(\w+)\("(\w+)"\)',
        sql,
        re.IGNORECASE
    )
    if match:
        table_name = match.group(1)
        csv_path = match.group(2)
        index_type = match.group(3).lower()
        indexed_column = match.group(4).lower()
        return ("create_from_csv", table_name, csv_path, index_type, indexed_column)

    # --- CREATE TABLE tradicional ---
    if sql.lower().startswith("create table"):
        match = re.match(r'create table\s+(\w+)\s*\((.+)\)', sql, re.IGNORECASE | re.DOTALL)
        if match:
            table_name = match.group(1)
            fields_block = match.group(2)

            fields_raw = [f.strip() for f in fields_block.split(',')]
            fields = []

            for field in fields_raw:
                f_match = re.match(
                    r'(\w+)\s+(\w+|\w+\[\w+\])(?:\s+PRIMARY\s+KEY)?(?:\s+INDEX\s+(\w+))?',
                    field,
                    re.IGNORECASE
                )
                if f_match:
                    field_dict = {
                        "name": f_match.group(1),
                        "type": f_match.group(2).upper()
                    }
                    if "PRIMARY KEY" in field.upper():
                        field_dict["primary"] = True
                    if f_match.group(3):
                        field_dict["index"] = f_match.group(3).upper()
                    fields.append(field_dict)

            return ("create", table_name, fields)

    # --- RANGE QUERY ---
    if "between" in sql.lower():
        match = re.search(r'where\s+(\w+)\s+between\s+([\d\.-]+)\s+and\s+([\d\.-]+)', sql, re.IGNORECASE)
        if match:
            return ("range", match.group(1), float(match.group(2)), float(match.group(3)))

    # --- SELECT WHERE = ---
    if sql.lower().startswith("select"):
        match = re.search(r'where\s+(\w+)\s*=\s*([\d\.-]+)', sql, re.IGNORECASE)
        if match:
            return ("search", match.group(1), float(match.group(2)))

    # --- INSERT INTO ---
    if sql.lower().startswith("insert into"):
        match = re.search(r'values\s*\((.+)\)', sql, re.IGNORECASE)
        if match:
            campos = [x.strip().strip("'").strip('"') for x in match.group(1).split(',')]
            return ("insert", campos)

    # --- DELETE ---
    if sql.lower().startswith("delete"):
        match = re.search(r'where\s+(\w+)\s*=\s*([\d\.-]+)', sql, re.IGNORECASE)
        if match:
            return ("delete", match.group(1), float(match.group(2)))

    return ("unknown",)
