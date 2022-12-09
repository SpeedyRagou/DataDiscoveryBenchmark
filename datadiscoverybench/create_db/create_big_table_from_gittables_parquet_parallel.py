import duckdb
import glob
from zipfile import ZipFile
import pyarrow.parquet as pq
import pandas as pd
import multiprocessing
from functools import partial
import os
import pickle

#path = "/home/neutatz/Software/DataDiscoveryBenchmark/data/git_parquet"
#path = "/home/mahdi/gittable2022/GitTable2022_parquet"

#my_path = '/home/neutatz/Software/DataDiscoveryBenchmark/data/new_git_parallel'
#my_path = '/home/felix/duckdb'

def zip2parquet(zip_path, my_path=None):
    if not os.path.isfile(my_path + '/gittables/import/' + zip_path.split('/')[-1].split('.')[0].replace('\'', '') + '.parquet'):
        cell_values = []
        table_ids = []
        column_ids = []
        row_ids = []

        with ZipFile(zip_path) as zf:
            for file in zf.namelist():
                if not file.endswith('.parquet'):  # optional filtering by filetype
                    continue
                try:
                    table_name = zip_path.split('/')[-1].split('.')[0].replace('\'', '') + '_' + file.split('.')[0]
                    # TODO:detect header
                    # TODO: unify representation of None / NAN / ...
                    df = pq.read_table(zf.open(file)).to_pandas()  # TODO:detect index column => save space
                    values = df.values
                    for row_id in range(values.shape[0]):
                        for column_id in range(values.shape[1]):
                            # cell_values.append(str(values[row_id, column_id]))
                            cell_values.append(str(values[row_id, column_id]).lower())
                            table_ids.append(table_name)
                            column_ids.append(column_id)
                            row_ids.append(row_id)
                except Exception as e:
                    print("error: " + str(zip_path + file) + ' -> ' + str(e))

        d = {'CellValue': cell_values, 'TableId': table_ids, 'ColumnId': column_ids, 'RowId': row_ids}
        df = pd.DataFrame(data=d)
        df['ColumnId'] = df['ColumnId'].astype('int')
        df['RowId'] = df['RowId'].astype('int')
        df.to_parquet(my_path + '/gittables/import/' + zip_path.split('/')[-1].split('.')[0].replace('\'', '') + '.parquet')


def create_db(dir_path, parts, con=None, store_db=True):
    try:
        if os.name == 'nt':
            multiprocessing.set_start_method('spawn')
        else:
            multiprocessing.set_start_method('fork')
    except:
        pass
    my_path = dir_path + '/data'

    paths = [dir_path + '/data/gittables/data/' + p + '.zip' for p in parts]

    if store_db:
        with open(dir_path + '/data/gittables/db/parts.pickle', "w+b") as output_file:
            pickle.dump(parts, output_file)

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    pool.map(partial(zip2parquet, my_path=my_path), paths)

    if type(con) == type(None):
        con = duckdb.connect(database=':memory:')
    con.execute("CREATE TABLE AllTables(CellValue VARCHAR, TableId VARCHAR, ColumnId USMALLINT, RowId UINTEGER);")
    parquet_files_str = '['
    for zip_path in paths:
        parquet_files_str += "'" + my_path + '/gittables/import/' + zip_path.split('/')[-1].split('.')[0].replace('\'', '') + '.parquet' + "', "
    parquet_files_str= parquet_files_str[:-2] + ']'
    con.execute("INSERT INTO AllTables SELECT * FROM read_parquet(" + parquet_files_str + ");")
    con.execute("CREATE INDEX token_idx ON AllTables (CellValue);")

    if store_db:
        con.execute("EXPORT DATABASE '" + my_path + "/gittables/db/' (FORMAT PARQUET);")
