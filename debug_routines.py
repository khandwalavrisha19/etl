"""
Debug script: connects to MySQL, extracts the 3 failing routines,
runs them through RoutineTransformer, and prints the result.
"""
import getpass, sys, pymysql
sys.path.insert(0, r"c:\Users\ruchi\ETL2")
from transform import RoutineTransformer

FAILING = {"archive_completed_orders", "process_all_pending_orders", "safe_delete_customer"}

host = input("MySQL host [localhost]: ").strip() or "localhost"
port = int(input("MySQL port [3306]: ").strip() or "3306")
user = input("MySQL user [root]: ").strip() or "root"
password = getpass.getpass("MySQL password: ")
database = input("MySQL database: ").strip()

conn = pymysql.connect(host=host, port=port, user=user, password=password,
                       database=database, charset="utf8mb4",
                       cursorclass=pymysql.cursors.DictCursor)
cur = conn.cursor()

transformer = RoutineTransformer()

# Procedures
cur.execute("""SELECT ROUTINE_NAME, ROUTINE_TYPE FROM INFORMATION_SCHEMA.ROUTINES
               WHERE ROUTINE_SCHEMA = %s""", (database,))
for row in cur.fetchall():
    name = row["ROUTINE_NAME"]
    rtype = row["ROUTINE_TYPE"]
    if name not in FAILING:
        continue
    if rtype == "PROCEDURE":
        cur.execute(f"SHOW CREATE PROCEDURE `{name}`")
        defn = cur.fetchone().get("Create Procedure", "")
        result = transformer.transform_routine(defn, is_function=False)
    else:
        cur.execute(f"SHOW CREATE FUNCTION `{name}`")
        defn = cur.fetchone().get("Create Function", "")
        result = transformer.transform_routine(defn, is_function=True)

    print(f"\n{'='*70}")
    print(f"  {rtype}: {name}")
    print(f"{'='*70}")
    print("--- MYSQL SOURCE ---")
    print(defn)
    print("--- TRANSFORMED ---")
    print(result)

conn.close()
