import duckdb
# to start an in-memory database
con = duckdb.connect(database=':memory:')

con.execute("IMPORT DATABASE '/home/neutatz/Software/DataDiscoveryBenchmark/data/db/';")


print(con.execute("SHOW TABLES;").fetchall())

print(con.execute("SELECT COUNT(*) FROM AllTables WHERE CellValue='US';").fetchall())
print('show number of tables: ' + str(con.execute("SELECT COUNT(DISTINCT TableId) AS some_alias FROM AllTables;").fetchall()))