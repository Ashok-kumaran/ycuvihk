from typing import Any, Dict
from mcp.server.fastmcp import FastMCP
from fastapi import Query, HTTPException, Body
from hdbcli import dbapi
import os
from dotenv import load_dotenv

load_dotenv()

mcp = FastMCP("S4HANA_DB")

HANA_HOST = os.getenv("HANA_HOST")
HANA_PORT = int(os.getenv("HANA_PORT", "443"))
HANA_USER = os.getenv("HANA_USER")
HANA_PASS = os.getenv("HANA_PASS")
HANA_SCHEMA = os.getenv("HANA_SCHEMA")

conn = dbapi.connect(
    address=HANA_HOST,
    port=HANA_PORT,
    user=HANA_USER,
    password=HANA_PASS,
    encrypt=True,
    sslValidateCertificate=False
)

@mcp.tool()
async def get_schema():
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE_NAME
            FROM SYS.TABLE_COLUMNS
            WHERE SCHEMA_NAME = '{HANA_SCHEMA}' AND TABLE_NAME = 'Customer'
        """)
        results = cursor.fetchall()
    finally:
        cursor.close()

    fields = [
        {"name": column_name, "type": data_type.lower()}
        for column_name, data_type in results
    ]

    return {
        "table": "Customer",
        "fields": fields
    }

@mcp.tool()
async def insert_data(
    table: str = Query(..., description="Table name"),
    data: Dict[str, Any] = Body(..., description="Column-value pairs")
):
    cursor = conn.cursor()
    try:
        columns = ', '.join(f'"{col}"' for col in data.keys())
        placeholders = ', '.join('?' for _ in data)
        values = list(data.values())
        sql = f'INSERT INTO "{HANA_SCHEMA}"."{table}" ({columns}) VALUES ({placeholders})'
        cursor.execute(sql, values)
        conn.commit()
    except dbapi.Error as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()

    return {"message": f"Row inserted into '{table}'", "data": data}

@mcp.tool()
async def get_data(
    table: str = Query(..., description="Table name")
):
    cursor = conn.cursor()
    try:
        cursor.execute(f'SELECT * FROM "{HANA_SCHEMA}"."{table}" LIMIT 100')
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    except dbapi.Error as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()

    return {
        "table": table,
        "rows": [dict(zip(columns, row)) for row in rows]
    }

if __name__ == "__main__":
    mcp.run(transport="stdio")
