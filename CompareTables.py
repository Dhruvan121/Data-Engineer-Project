import pyodbc

def get_connection(server, database):
    connection_string = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};Trusted_Connection=yes'
    return pyodbc.connect(connection_string)

def get_schemas(connection):
    query = """
    SELECT SCHEMA_NAME
    FROM INFORMATION_SCHEMA.SCHEMATA
    WHERE SCHEMA_NAME NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin',
                              'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader',
                              'db_datawriter', 'db_denydatareader', 'db_denydatawriter')
    """
    cursor = connection.cursor()
    cursor.execute(query)
    return set(row.SCHEMA_NAME for row in cursor.fetchall())

def get_tables(connection, schema):
    query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?"
    cursor = connection.cursor()
    cursor.execute(query, (schema,))
    return set(row.TABLE_NAME.lower() for row in cursor.fetchall())

def get_columns(connection, schema, table):
    query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
    cursor = connection.cursor()
    cursor.execute(query, (schema, table))
    return set(row.COLUMN_NAME for row in cursor.fetchall())

def get_row_count(connection, schema, table):
    query = f"SELECT COUNT(*) FROM [{schema}].[{table}]"
    cursor = connection.cursor()
    cursor.execute(query)
    row_count = cursor.fetchone()[0]
    return schema, table, row_count

def compare_servers(server1, database1, server2, database2):
    connection1 = get_connection(server1, database1)
    connection2 = get_connection(server2, database2)

    schemas1 = get_schemas(connection1)
    schemas2 = get_schemas(connection2)

    different_schemas = schemas1.symmetric_difference(schemas2)
    common_schemas = schemas1.intersection(schemas2)

    different_tables_list = []
    common_tables_list = []
    same_column_list = []
    different_column_list = []
    same_count_list = []
    different_count_list = []
    
    for schema in common_schemas:
        tables1 = get_tables(connection1, schema)
        tables2 = get_tables(connection2, schema)

        different_tables = tables1.symmetric_difference(tables2)
        different_tables_list.extend(different_tables)

        common_tables = tables1.intersection(tables2)
        common_tables_list.extend(common_tables)

        for table in common_tables:
            columns1 = get_columns(connection1, schema, table)
            columns2 = get_columns(connection2, schema, table)

            different_columns = columns1.symmetric_difference(columns2)
            if different_columns:
                different_column_list.append((table,different_columns))
            else:
                same_column_list.append(table)
        
            row_count1 = get_row_count(connection1, schema, table)
            row_count2 = get_row_count(connection2, schema, table)

            if row_count1 != row_count2:
                different_count_list.append((row_count1[0],row_count1[1],row_count1[-1],row_count2[-1]))
               
            else:
                same_count_list.append((row_count1))

    connection1.close()
    connection2.close()

    print("\nDifferent Schemas:")
    print(different_schemas)
    
    print("\nCommon Schemas:")
    print(common_schemas)

    print("\nDifferent Tables:")
    print(different_tables_list)
    
    print("\nCommon Tables:")
    print(common_tables_list)
    
    print("\ndifferent columns:")
    print(different_column_list)
    
    print("\ncommon columns")
    print(same_column_list)
    
    print("...........................................................................")
    print("\nsame row count:")
    print(same_count_list)
    
    print("...........................................................................")
    print("\ndifferent row count:")
    print(different_count_list)

    

if __name__ == "__main__":
    server1 = 'localhost,1433'
    database1 = 'AdventureWorks2022'

    server2 = 'localhost,1433'
    database2 = 'AdventureWorks2022'

    compare_servers(server1, database1, server2, database2)
