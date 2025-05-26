import re

def parse_sql(sql: str):
    sql = sql.strip()

    if sql.lower().startswith("create table"):
        match = re.search(
            r'create table (\w+)\s+from file\s+[\'"](.+?)[\'"]\s+using index\s+(\w+)\s*\(\s*[\'"]?(\w+)[\'"]?\s*\)',
            sql, re.IGNORECASE)
        if match:
            return ("create", match.group(1), match.group(2), match.group(3).lower(), match.group(4))

    if "between" in sql.lower():
        match = re.search(r'where\s+(\w+)\s+between\s+([\d\.-]+)\s+and\s+([\d\.-]+)', sql, re.IGNORECASE)
        if match:
            return ("range", match.group(1), float(match.group(2)), float(match.group(3)))

    if sql.lower().startswith("select"):
        match = re.search(r'where\s+(\w+)\s*=\s*([\d\.-]+)', sql, re.IGNORECASE)
        if match:
            return ("search", match.group(1), float(match.group(2)))

    if sql.lower().startswith("insert into"):
        match = re.search(r'values\s*\((.+)\)', sql, re.IGNORECASE)
        if match:
            campos = [x.strip().strip("'").strip('"') for x in match.group(1).split(',')]
            return ("insert", campos)

    if sql.lower().startswith("delete"):
        match = re.search(r'where\s+(\w+)\s*=\s*([\d\.-]+)', sql, re.IGNORECASE)
        if match:
            return ("delete", match.group(1), float(match.group(2)))

    return ("unknown",)
