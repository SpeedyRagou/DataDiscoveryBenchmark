import duckdb
import glob
import pandas as pd
import gzip
import json

cell_values = []
table_ids = []
column_ids = []
row_ids = []

path = '/home/mahdi/DWTC_json'
my_path = '/home/felix/duckdb'

#path = '/home/neutatz/Software/DataDiscoveryBenchmark/data/dresden'
#my_path = '/home/neutatz/Software/DataDiscoveryBenchmark/data'

table_id = 0

data = []
#download http://wwwdb.inf.tu-dresden.de/misc/dwtc/data_feb15/dwtc-000.json.gz
for zip_path in glob.glob(path + "/*.json.gz"):
    with gzip.open(zip_path, 'rt') as f:
        for line in f:
            json_data = json.loads(line)

            #print(json_data)

            #todo fix table header situation
            for row_id in range(len(json_data['relation'])):
                for column_id in range(len(json_data['relation'][0])):
                    cell_values.append(str(json_data['relation'][row_id][column_id]))
                    table_ids.append(table_id)
                    column_ids.append(column_id)
                    row_ids.append(row_id)
            table_id += 1

d = {'CellValue': cell_values, 'TableId': table_ids, 'ColumnId': column_ids, 'RowId': row_ids}
df = pd.DataFrame(data=d)

df['TableId'] = df['TableId'].astype('int')
df['ColumnId'] = df['ColumnId'].astype('int')
df['RowId'] = df['RowId'].astype('int')
df.to_parquet(my_path + '/dresden/import/all.parquet')

con = duckdb.connect(database=':memory:')
con.execute("CREATE TABLE AllTables AS SELECT * FROM '" + my_path + "/dresden/import/*.parquet';")
con.execute("CREATE INDEX token_idx ON AllTables (CellValue);")
con.execute("EXPORT DATABASE '" + my_path + "/dresden/db/' (FORMAT PARQUET);")