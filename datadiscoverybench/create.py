import duckdb
import glob

con = duckdb.connect(database=':memory:')


for csv_path in glob.glob("/home/neutatz/Software/DataDiscoveryBenchmark/data/git_tables_small/*.csv"):
    try:
        table_name = csv_path.split('/')[-1].split('.')[0]
        print(table_name)
        con.execute("CREATE TABLE " + str(table_name) + " AS SELECT * FROM read_csv_auto('" + csv_path + "', delim=',', header=True);")
    except:
        pass

print(con.execute("SHOW TABLES;").fetchall())

con.execute("EXPORT DATABASE '/home/neutatz/Software/DataDiscoveryBenchmark/data/db/' (FORMAT PARQUET);")