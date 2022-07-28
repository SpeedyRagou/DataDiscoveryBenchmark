import duckdb
import glob
import pandas as pd
import gzip
import json
import multiprocessing
from functools import partial
import os
import pickle

#path = '/home/mahdi/DWTC_json'
#my_path = '/home/felix/duckdb'

#path = '/home/neutatz/Software/DataDiscoveryBenchmark/data/dresden'
#my_path = '/home/felix/duckdb'



def zip2parquet(zip_path, my_path=None):
    if not os.path.isfile(my_path + '/dresden/import/' + zip_path.split('/')[-1].split('.')[0] + '.parquet'):
        cell_values = []
        table_ids = []
        column_ids = []
        row_ids = []

        with gzip.open(zip_path, 'rt') as f:
            for line in f:
                json_data = json.loads(line)
                #print(json_data['id'])
                #print(json_data)

                #todo fix table header situation
                for row_id in range(len(json_data['relation'])):
                    for column_id in range(len(json_data['relation'][0])):
                        cell_values.append(str(json_data['relation'][row_id][column_id]))
                        table_ids.append(json_data['tableNum'])
                        column_ids.append(column_id)
                        row_ids.append(row_id)


        d = {'CellValue': cell_values, 'TableId': table_ids, 'ColumnId': column_ids, 'RowId': row_ids}
        df = pd.DataFrame(data=d)
        df.to_parquet(my_path + '/dresden/import/' + zip_path.split('/')[-1].split('.')[0] + '.parquet')

def create_db(dir_path, parts):
    if os.name == 'nt':
        multiprocessing.set_start_method('spawn')
    else:
        multiprocessing.set_start_method('fork')
    my_path = dir_path + '/data'

    paths = [my_path + "/dresden/data/dwtc-" + str(p).zfill(3) + ".json.gz" for p in parts]

    with open(dir_path + '/data/dresden/db/parts.pickle', "w+b") as output_file:
        pickle.dump(parts, output_file)


    con = duckdb.connect(database=':memory:')

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    pool.map(partial(zip2parquet, my_path=my_path), paths)

    con.execute("CREATE TABLE AllTables(CellValue VARCHAR, TableId UINTEGER, ColumnId USMALLINT, RowId UINTEGER);")
    parquet_files_str = '['
    for zip_path in paths:
        parquet_files_str += "'" + my_path + '/dresden/import/' + zip_path.split('/')[-1].split('.')[0] + '.parquet' + "', "
    parquet_files_str = parquet_files_str[:-2] + ']'
    con.execute("INSERT INTO AllTables SELECT * FROM read_parquet(" + parquet_files_str + ");")
    con.execute("CREATE INDEX token_idx ON AllTables (CellValue);")
    con.execute("EXPORT DATABASE '" + my_path + "/dresden/db/' (FORMAT PARQUET);")
