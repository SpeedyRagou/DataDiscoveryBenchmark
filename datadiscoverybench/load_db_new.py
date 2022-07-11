import duckdb
import time
import pickle
# to start an in-memory database
con = duckdb.connect(database=':memory:')

start = time.time()

#my_path = '/home/neutatz/Software/DataDiscoveryBenchmark/data'
my_path = '/home/felix/duckdb'


con.execute("CREATE TABLE AllTables(CellValue UINTEGER, TableId UINTEGER, ColumnId USMALLINT, RowId UINTEGER);")
con.execute("INSERT INTO AllTables SELECT * FROM read_parquet('" + my_path + "/dresden/import/*.parquet');")
con.execute("CREATE INDEX token_idx ON AllTables (CellValue);")
con.execute("EXPORT DATABASE '" + my_path + "/dresden/db/' (FORMAT PARQUET);")

print('schema: ' + str(con.execute("DESCRIBE AllTables;").fetchall()))


print('max table_id: ' + str(con.execute("SELECT MAX(TableId) AS some_alias FROM AllTables;").fetchall()))
print('max cell value: ' + str(con.execute("SELECT MAX(CellValue) AS some_alias FROM AllTables;").fetchall()))
print('max ColumnId: ' + str(con.execute("SELECT MAX(ColumnId) AS some_alias FROM AllTables;").fetchall()))
print('max RowId: ' + str(con.execute("SELECT MAX(RowId) AS some_alias FROM AllTables;").fetchall()))