import duckdb
import time
import pickle
# to start an in-memory database
con = duckdb.connect(database=':memory:')

start = time.time()
my_path = '/home/felix/duckdb'

#con.execute("IMPORT DATABASE '/home/neutatz/Software/DataDiscoveryBenchmark/data/db/';")
#con.execute("IMPORT DATABASE '/home/felix/duckdb/gittables/db/';")
con.execute("CREATE TABLE AllTables AS SELECT * FROM '" + my_path + "/dresden/import/*.parquet';")

print('time to load the db: ' + str(time.time() - start))


#print(con.execute("SELECT COUNT(*) FROM AllTables WHERE CellValue='US';").fetchall())
print('show number of tables: ' + str(con.execute("SELECT COUNT(DISTINCT TableId) AS some_alias FROM AllTables;").fetchall()))
#print(len(data))

print('max table_id: ' + str(con.execute("SELECT MAX(TableId) AS some_alias FROM AllTables;").fetchall()))
print('max cell value: ' + str(con.execute("SELECT MAX(CellValue) AS some_alias FROM AllTables;").fetchall()))
print('max ColumnId: ' + str(con.execute("SELECT MAX(ColumnId) AS some_alias FROM AllTables;").fetchall()))
print('max RowId: ' + str(con.execute("SELECT MAX(RowId) AS some_alias FROM AllTables;").fetchall()))
