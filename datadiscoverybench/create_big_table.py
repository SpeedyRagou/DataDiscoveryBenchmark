import duckdb
import glob
import pandas as pd

con = duckdb.connect(database=':memory:')

d = {'col1': [1, 2], 'col2': [3, 4]}

cell_values = []
table_ids = []
column_ids = []
row_ids = []


for csv_path in glob.glob("/home/neutatz/Software/DataDiscoveryBenchmark/data/git_tables_small/*.csv"):
    try:
        table_name = csv_path.split('/')[-1].split('.')[0]
        df = pd.read_csv(csv_path, delimiter=',', index_col=0)
        values = df.values
        print(values)
        for row_id in range(values.shape[0]):
            for column_id in range(values.shape[1]):
                cell_values.append(str(values[row_id,column_id]))
                table_ids.append(table_name.split('_')[-1])
                column_ids.append(column_id)
                row_ids.append(row_id)
    except:
        pass

d = {'CellValue': cell_values, 'TableId': table_ids, 'ColumnId': column_ids, 'RowId': row_ids}
df = pd.DataFrame(data=d)
df.to_parquet('/home/neutatz/Software/DataDiscoveryBenchmark/data/big_csv/all.parquet')
print(df.head(10))

con.execute("CREATE TABLE AllTables AS SELECT * FROM '/home/neutatz/Software/DataDiscoveryBenchmark/data/big_csv/all.parquet';")

print(con.execute("SHOW TABLES;").fetchall())

print(con.execute("CREATE INDEX token_idx ON AllTables (CellValue);"))

con.execute("EXPORT DATABASE '/home/neutatz/Software/DataDiscoveryBenchmark/data/db/' (FORMAT PARQUET);")