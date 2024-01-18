import pyodbc

def get_connection(server, database):
    connection_string = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};Trusted_Connection=yes'
    return pyodbc.connect(connection_string)

def get_schemas(connection):
    query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT LIKE 'db_%' AND SCHEMA_NAME != 'guest' AND SCHEMA_NAME != 'INFORMATION_SCHEMA' AND SCHEMA_NAME != 'sys';"
    cursor = connection.cursor()
    cursor.execute(query)
    return [schema.SCHEMA_NAME for schema in cursor.fetchall()]

def get_tables(connection, schema):
    query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}'"
    cursor = connection.cursor()
    cursor.execute(query)
    return [table.TABLE_NAME for table in cursor.fetchall()]

def get_row_count(connection, schema, table):
    query = f"SELECT COUNT(*) FROM {schema}.{table}"
    cursor = connection.cursor()
    cursor.execute(query)
    row_count = cursor.fetchone()[0]
    return row_count

def get_db_columns(connection, table_name):
    cursor = connection.cursor()
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
    cursor.execute(query)
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return columns

def compare_schemas_and_tables(server, database):
    try:
        connection = get_connection(server, database)
        
        schemas = get_schemas(connection)
        print("All schemas", schemas)

        for schema in schemas:
            print(f"\nSchema: {schema}")

            tables = get_tables(connection, schema)
            print("List of tables", tables)

            for table in tables:
                table_columns = get_db_columns(connection, table)
                print(f"Table Columns for {table}: {table_columns}")
                row_count = get_row_count(connection, schema, table)
                print(f"Row count for {table}: {row_count}")

    except Exception as e:
        print("Error:", e)
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    server = 'localhost,1433'
    database = 'test'

    compare_schemas_and_tables(server, database)
