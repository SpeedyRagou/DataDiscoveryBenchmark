import duckdb
import time
# to start an in-memory database
con = duckdb.connect(database=':memory:')

start = time.time()

#con.execute("IMPORT DATABASE '/home/neutatz/Software/DataDiscoveryBenchmark/data/db/';")
con.execute("IMPORT DATABASE '/home/felix/duckdb/gittables/db/';")

print('time to load the db: ' + str(time.time() - start))


print(con.execute("SHOW TABLES;").fetchall())

print(con.execute("SELECT COUNT(*) FROM AllTables WHERE CellValue='US';").fetchall())
print('show number of tables: ' + str(con.execute("SELECT COUNT(DISTINCT TableId) AS some_alias FROM AllTables;").fetchall()))