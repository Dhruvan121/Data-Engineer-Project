import pyodbc
import pandas as pd

db_conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=test;Trusted_Connection=yes'
conn = None 
try:
    conn = pyodbc.connect(db_conn_str)
    sql_query = "SELECT * FROM dbo.Product"
    df = pd.read_sql(sql_query, conn)
    print(df)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if conn:
        conn.close()

