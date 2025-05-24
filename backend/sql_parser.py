import re

def parse_sql(sql: str):
    sql = sql.strip().lower()
    if sql.startswith("insert into"):
        val = int(re.search(r'values\s*\((\d+)\)', sql).group(1))
        return ("insert", val)
    elif sql.startswith("select") and "between" in sql:
        nums = list(map(int, re.findall(r'\d+', sql)))
        return ("range_search", nums[0], nums[1])
    elif sql.startswith("select"):
        key = int(re.search(r'where\s+\w+\s*=\s*(\d+)', sql).group(1))
        return ("search", key)
    elif sql.startswith("delete"):
        key = int(re.search(r'where\s+\w+\s*=\s*(\d+)', sql).group(1))
        return ("delete", key)
    else:
        return ("unknown",)
