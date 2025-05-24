import re

def parse_sql(sql: str):
    sql = sql.strip()

    if sql.lower().startswith("insert into"):
        match = re.search(r'values\s*\(\s*(\d+)\s*\)', sql, re.IGNORECASE)
        if match:
            return ("insert", int(match.group(1)))

    elif sql.lower().startswith("select") and "between" in sql.lower():
        match = re.search(r'between\s+(\d+)\s+and\s+(\d+)', sql, re.IGNORECASE)
        if match:
            return ("range_search", int(match.group(1)), int(match.group(2)))

    elif sql.lower().startswith("select"):
        # Soporte para strings entre comillas o números
        match = re.search(r'where\s+(\w+)\s*=\s*(?:[\'"])?(.+?)(?:[\'"])?\s*$', sql, re.IGNORECASE)
        if match:
            field = match.group(1)
            value = match.group(2)
            return ("search", field, value)

    elif sql.lower().startswith("delete"):
        match = re.search(r'where\s+\w+\s*=\s*(\d+)', sql, re.IGNORECASE)
        if match:
            return ("delete", int(match.group(1)))

    elif sql.lower().startswith("create table") and "from file" in sql.lower():
        # Caso completo: con índice
        match_full = re.search(
            r'create table (\w+)\s+from file\s+[\'"](.+?)[\'"]\s+using index\s+(\w+)\s*\(\s*[\'"]?(\w+)[\'"]?\s*\)',
            sql,
            re.IGNORECASE
        )
        if match_full:
            return ("create_from_file", match_full.group(1), match_full.group(2), match_full.group(3).lower(), match_full.group(4))

        # Caso simple: sin índice
        match_simple = re.search(
            r'create table (\w+)\s+from file\s+[\'"](.+?)[\'"]',
            sql,
            re.IGNORECASE
        )
        if match_simple:
            return ("load_file_only", match_simple.group(1), match_simple.group(2))

    return ("unknown",)
