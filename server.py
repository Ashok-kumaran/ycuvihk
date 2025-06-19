# server.py
from hdbcli import dbapi
from loguru import logger
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
import os

load_dotenv()

mcp = FastMCP("HANACloudDemo")

HANA_CONFIG = {
    "address": os.getenv("HANA_HOST"),
    "port": int(os.getenv("HANA_PORT")),
    "user": os.getenv("HANA_USER"),
    "password": os.getenv("HANA_PASSWORD"),
    "encrypt": "true",
    "sslValidateCertificate": "false",
}

@mcp.tool()
def query_data(sql: str) -> str:
    """Executes SQL queries on SAP HANA Cloud"""
    logger.info(f"Executing SQL query on HANA: {sql}")
    try:
        conn = dbapi.connect(**HANA_CONFIG)
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return "\n".join(str(row) for row in result)
    except dbapi.Error as e:
        return f"HANA DB Error: {str(e)}"
    except Exception as e:
        return f"General Error: {str(e)}"

@mcp.prompt()
def example_prompt(code: str) -> str:
    return f"Please review this code:\n\n{code}"

if __name__ == "__main__":
    print("Starting server with HANA backend...")
    mcp.run(transport="stdio")
