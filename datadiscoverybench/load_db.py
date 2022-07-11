import duckdb
import time
import pickle
# to start an in-memory database
con = duckdb.connect(database=':memory:')

start = time.time()

#con.execute("IMPORT DATABASE '/home/neutatz/Software/DataDiscoveryBenchmark/data/db/';")
#con.execute("IMPORT DATABASE '/home/felix/duckdb/gittables/db/';")
con.execute("IMPORT DATABASE '/home/felix/duckdb/dresden/db/';")

print('time to load the db: ' + str(time.time() - start))

path = '/home/mahdi/DWTC_json'
my_path = '/home/felix/duckdb'

#path = '/home/neutatz/Software/DataDiscoveryBenchmark/data/dresden'
#my_path = '/home/neutatz/Software/DataDiscoveryBenchmark/data'

data = pickle.load(open(my_path + '/dresden/import/dict.pickle', 'rb'))

print(con.execute("SHOW TABLES;").fetchall())



#print(con.execute("SELECT COUNT(*) FROM AllTables WHERE CellValue='US';").fetchall())
print('show number of tables: ' + str(con.execute("SELECT COUNT(DISTINCT TableId) AS some_alias FROM AllTables;").fetchall()))
print(len(data))

print('max table_id: ' + str(con.execute("SELECT MAX(TableId) AS some_alias FROM AllTables;").fetchall()))
print('max cell value: ' + str(con.execute("SELECT MAX(CellValue) AS some_alias FROM AllTables;").fetchall()))
print('max ColumnId: ' + str(con.execute("SELECT MAX(ColumnId) AS some_alias FROM AllTables;").fetchall()))
print('max RowId: ' + str(con.execute("SELECT MAX(RowId) AS some_alias FROM AllTables;").fetchall()))
