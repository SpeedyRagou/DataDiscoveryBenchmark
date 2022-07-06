import duckdb
import glob
from zipfile import ZipFile
import pyarrow.parquet as pq
import pandas as pd
import multiprocessing

#path = "/home/neutatz/Software/DataDiscoveryBenchmark/data/git_parquet"
path = "/home/mahdi/gittable2022/GitTable2022_parquet"

#my_path = '/home/neutatz/Software/DataDiscoveryBenchmark/data/new_git_parallel'
my_path = '/home/felix/duckdb'

def zip2parquet(zip_path):
    cell_values = []
    table_ids = []
    column_ids = []
    row_ids = []

    with ZipFile(zip_path) as zf:
        for file in zf.namelist():
            if not file.endswith('.parquet'):  # optional filtering by filetype
                continue
            try:
                table_name = zip_path.split('/')[-1].split('.')[0] + '_' + file.split('.')[0]
                # TODO:detect header
                # TODO: unify representation of None / NAN / ...
                df = pq.read_table(zf.open(file)).to_pandas()  # TODO:detect index column => save space
                values = df.values
                for row_id in range(values.shape[0]):
                    for column_id in range(values.shape[1]):
                        cell_values.append(str(values[row_id, column_id]))
                        table_ids.append(table_name)
                        column_ids.append(column_id)
                        row_ids.append(row_id)
            except Exception as e:
                print("error: " + str(zip_path + file) + ' -> ' + str(e))

    d = {'CellValue': cell_values, 'TableId': table_ids, 'ColumnId': column_ids, 'RowId': row_ids}
    df = pd.DataFrame(data=d)
    df.to_parquet(my_path + '/gittables/import/' + zip_path.split('/')[-1].split('.')[0] + '.parquet')

con = duckdb.connect(database=':memory:')

pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
pool.map(zip2parquet, glob.glob(path + "/*.zip"))

con.execute("CREATE TABLE AllTables AS SELECT * FROM '" + my_path + "/gittables/import/*.parquet';")
con.execute("CREATE INDEX token_idx ON AllTables (CellValue);")
con.execute("EXPORT DATABASE '" + my_path + "/gittables/db/' (FORMAT PARQUET);")
