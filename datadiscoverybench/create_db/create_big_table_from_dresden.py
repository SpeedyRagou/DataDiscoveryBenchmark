import duckdb
import glob
import pandas as pd
import gzip
import json

con = duckdb.connect(database=':memory:')

cell_values = []
table_ids = []
column_ids = []
row_ids = []

data = []
#download http://wwwdb.inf.tu-dresden.de/misc/dwtc/data_feb15/dwtc-000.json.gz
with gzip.open('/home/neutatz/Downloads/dwtc-000.json.gz', 'rt') as f:
    for line in f:
        json_data = json.loads(line)

        print(json_data)

        #todo fix table header situation
        for row_id in range(len(json_data['relation'])):
            for column_id in range(len(json_data['relation'][0])):
                cell_values.append(str(json_data['relation'][row_id][column_id]))
                table_ids.append(json_data['tableNum'])
                column_ids.append(column_id)
                row_ids.append(row_id)

d = {'CellValue': cell_values, 'TableId': table_ids, 'ColumnId': column_ids, 'RowId': row_ids}
df = pd.DataFrame(data=d)
df.to_parquet('/home/neutatz/Software/DataDiscoveryBenchmark/data/big_csv/all.parquet')
print(df.head(10))

con.execute("CREATE TABLE AllTables AS SELECT * FROM '/home/neutatz/Software/DataDiscoveryBenchmark/data/big_csv/all.parquet';")

print(con.execute("SHOW TABLES;").fetchall())

print(con.execute("CREATE INDEX token_idx ON AllTables (CellValue);"))

con.execute("EXPORT DATABASE '/home/neutatz/Software/DataDiscoveryBenchmark/data/db/' (FORMAT PARQUET);")