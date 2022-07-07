import duckdb

path = '/home/mahdi/DWTC_json'
my_path = '/home/felix/duckdb'

#path = '/home/neutatz/Software/DataDiscoveryBenchmark/data/dresden'
#my_path = '/home/neutatz/Software/DataDiscoveryBenchmark/data'


con = duckdb.connect(database=':memory:')
con.execute("CREATE TABLE AllTables AS SELECT * FROM '" + my_path + "/dresden/import/*.parquet';")
con.execute("CREATE INDEX token_idx ON AllTables (CellValue);")
con.execute("EXPORT DATABASE '" + my_path + "/dresden/db/' (FORMAT PARQUET);")