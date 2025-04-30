# instrument_routes.py
import os
import duckdb
import tempfile
import requests
import pandas as pd
from datetime import datetime
from fastapi import APIRouter
from fastapi.responses import JSONResponse

csv_url = "https://images.dhan.co/api-data/api-scrip-master.csv"
DB_FILE = "instruments.duckdb"

router = APIRouter()

def load_csv_to_duckdb():
    response = requests.get(csv_url)

    with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', encoding='utf-8') as temp_file:
        temp_file.write(response.text)
        temp_file.flush()
        temp_file_name = temp_file.name

    df = pd.read_csv(temp_file_name)
    df.columns = [col.strip() for col in df.columns]
    df = df[["SEM_CUSTOM_SYMBOL", "SEM_INSTRUMENT_NAME", "SEM_SMST_SECURITY_ID"]]
    df['DATE'] = datetime.today().strftime('%Y-%m-%d')

    con = duckdb.connect(DB_FILE)
    con.execute("DROP TABLE IF EXISTS instruments")
    con.register('df_view', df)
    con.execute("CREATE TABLE instruments AS SELECT * FROM df_view")
    con.close()
    os.remove(temp_file_name)

@router.get("/search-instruments")
def search_instruments(query: str):
    try:
        if not query.strip():
            return JSONResponse(content={"error": "Query is empty"}, status_code=400)

        con = duckdb.connect(DB_FILE)
        words = query.strip().split()
        where_clauses = " AND ".join(
            ["(SEM_CUSTOM_SYMBOL || ' ' || SEM_INSTRUMENT_NAME) ILIKE ?" for _ in words]
        )
        params = [f"%{word}%" for word in words]

        sql_query = f"""
        SELECT SEM_CUSTOM_SYMBOL, SEM_INSTRUMENT_NAME, SEM_SMST_SECURITY_ID, DATE
        FROM instruments
        WHERE {where_clauses}
        ORDER BY 
            (DATE = CURRENT_DATE) DESC,
            DATE DESC,
            ABS(CAST(DATE AS DATE) - CURRENT_DATE) ASC,
            SEM_CUSTOM_SYMBOL
        LIMIT 20
        """

        result = con.execute(sql_query, params).fetchall()
        con.close()

        instruments = [
            {
                "custom_symbol": row[0],
                "instrument_name": row[1],
                "security_id": row[2],
                "date": row[3]
            }
            for row in result
        ]

        return {"instruments": instruments}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
