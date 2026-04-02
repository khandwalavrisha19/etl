import pymysql

conn = pymysql.connect(host='localhost', port=3306, user='root', password='', database='etl_migration_db', cursorclass=pymysql.cursors.DictCursor)
cursor = conn.cursor()

# Get routines
cursor.execute("""
    SELECT ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_DEFINITION 
    FROM INFORMATION_SCHEMA.ROUTINES 
    WHERE ROUTINE_SCHEMA = 'etl_migration_db'
""")
routines = cursor.fetchall()

for r in routines:
    print(f"=== {r['ROUTINE_NAME']} ({r['ROUTINE_TYPE']}) ===")
    
    # Get parameters
    cursor.execute("""
        SELECT PARAMETER_NAME, DATA_TYPE, PARAMETER_MODE
        FROM INFORMATION_SCHEMA.PARAMETERS
        WHERE SPECIFIC_SCHEMA = 'etl_migration_db' 
        AND SPECIFIC_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (r['ROUTINE_NAME'],))
    params = cursor.fetchall()
    print("Params:", params)
    print("Body:", r['ROUTINE_DEFINITION'][:100].replace('\n', ' '))

conn.close()
