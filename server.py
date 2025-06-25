from typing import Any
from mcp.server.fastmcp import FastMCP
from fastapi import FastAPI, Query, HTTPException, Body
from hdbcli import dbapi
from typing import Dict, Any
import os
from dotenv import load_dotenv
 
# Load environment variables
load_dotenv()
 
# Initialize FastMCP server
mcp = FastMCP("S4HANA_DB")
 
 
HANA_HOST = os.getenv("HANA_HOST")
HANA_PORT = int(os.getenv("HANA_PORT", "443"))
HANA_USER = os.getenv("HANA_USER")
HANA_PASS = os.getenv("HANA_PASS")
HANA_SCHEMA = os.getenv("HANA_SCHEMA")
 
# Connect to SAP HANA Cloud
conn = dbapi.connect(
    address=HANA_HOST,
    port=HANA_PORT,
    user=HANA_USER,
    password=HANA_PASS,
    encrypt=True,
    sslValidateCertificate=False
)
 
#Implementing tool execution
@mcp.tool()
async def get_schema():
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE_NAME
            FROM SYS.TABLE_COLUMNS
            WHERE SCHEMA_NAME = '{HANA_SCHEMA}'
        """)
        results = cursor.fetchall()
    finally:
        cursor.close()
 
    schema: Dict[str, Dict] = {}
    for table_name, column_name, data_type in results:
        if table_name not in schema:
            schema[table_name] = {"type": "table", "fields": []}
        schema[table_name]["fields"].append({
            "name": column_name,
            "type": data_type.lower()
        })
 
    return {
        "version": "1.0",
        "schema": schema
    }
 
@mcp.tool()
async def get_data(table: str = Query(..., description="SAP HANA table name")):
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
        "object": "list",
        "table": table,
        "rows": [dict(zip(columns, row)) for row in rows]
    }
 
@mcp.tool()
def insert_data(
    table: str = Query(..., description="SAP HANA table name"),
    data: Dict[str, Any] = Body(..., description="Column-value pairs to insert")
):
    cursor = conn.cursor()
    try:
        col_clause = ', '.join(f'"{col}"' for col in data.keys())
        val_clause = ', '.join(['?' for _ in data])
        values = list(data.values())
 
        sql = f'INSERT INTO "{HANA_SCHEMA}"."{table}" ({col_clause}) VALUES ({val_clause})'
        cursor.execute(sql, values)
        conn.commit()
    except dbapi.Error as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
 
    return {
        "object": "insert_result",
        "message": f"Successfully inserted row into '{table}'",
        "data": data
    }
 
@mcp.tool()
async def delete_data(
    table: str = Query(..., description="SAP HANA table name"),
    where: dict = Body(..., description="WHERE clause column-value pairs")
):
    cursor = conn.cursor()
    try:
        where_clause = ' AND '.join(f'"{col}" = ?' for col in where.keys())
        values = list(where.values())
        sql = f'DELETE FROM "{HANA_SCHEMA}"."{table}" WHERE {where_clause}'
        cursor.execute(sql, values)
        conn.commit()
    except dbapi.Error as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
    return {
        "object": "delete_result",
        "message": f"Successfully deleted row(s) from '{table}'",
        "where": where
    }
 
@mcp.tool()
async def update_data(
    table: str = Query(..., description="SAP HANA table name"),
    data: dict = Body(..., description="Column-value pairs to update"),
    where: dict = Body(..., description="WHERE clause column-value pairs")
):
    cursor = conn.cursor()
    try:
        set_clause = ', '.join(f'"{col}" = ?' for col in data.keys())
        where_clause = ' AND '.join(f'"{col}" = ?' for col in where.keys())
        values = list(data.values()) + list(where.values())
        sql = f'UPDATE "{HANA_SCHEMA}"."{table}" SET {set_clause} WHERE {where_clause}'
        cursor.execute(sql, values)
        conn.commit()
    except dbapi.Error as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
    return {
        "object": "update_result",
        "message": f"Successfully updated row(s) in '{table}'",
        "data": data,
        "where": where
    }
 
 
 
#Running the server
if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')