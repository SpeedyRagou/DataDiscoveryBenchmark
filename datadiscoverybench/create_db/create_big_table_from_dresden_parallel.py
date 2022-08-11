import duckdb
import pandas as pd
import gzip
import json
import multiprocessing
from functools import partial
import os
import pickle
from multiprocessing import Manager
from datadiscoverybench.create_db.lock_utils import my_lock

#path = '/home/mahdi/DWTC_json'
#my_path = '/home/felix/duckdb'

#path = '/home/neutatz/Software/DataDiscoveryBenchmark/data/dresden'
#my_path = '/home/felix/duckdb'



def zip2parquet(zip_path, my_path=None, table_num_dict=None):
    cell_values = []
    table_ids = []
    column_ids = []
    row_ids = []

    budget = 0
    with gzip.open(zip_path, 'rt') as f0:
        for _ in f0:
            budget += 1

    current_pointer = None
    my_lock.acquire()
    try:
        current_pointer = table_num_dict['table_num']
        table_num_dict['table_num'] += 1
    except Exception as e:
        print('catched: ' + str(e))
    finally:
        my_lock.release()


    with gzip.open(zip_path, 'rt') as f:
        for line in f:
            json_data = json.loads(line)
            #print(json_data['id'])
            #print(json_data)

            #todo fix table header situation
            for row_id in range(len(json_data['relation'])):
                for column_id in range(len(json_data['relation'][0])):
                    cell_values.append(str(json_data['relation'][row_id][column_id]))
                    table_ids.append(current_pointer)
                    column_ids.append(column_id)
                    row_ids.append(row_id)

            current_pointer += 1


    d = {'CellValue': cell_values, 'TableId': table_ids, 'ColumnId': column_ids, 'RowId': row_ids}
    df = pd.DataFrame(data=d)
    df.to_parquet(my_path + '/dresden/import/' + zip_path.split('/')[-1].split('.')[0] + '.parquet')

def create_db(dir_path, parts, con=None, store_db=True):
    try:
        if os.name == 'nt':
            multiprocessing.set_start_method('spawn')
        else:
            multiprocessing.set_start_method('fork')
    except:
        pass

    my_path = dir_path + '/data'

    paths = [my_path + "/dresden/data/dwtc-" + str(p).zfill(3) + ".json.gz" for p in parts]

    if store_db:
        with open(dir_path + '/data/dresden/db/parts.pickle', "w+b") as output_file:
            pickle.dump(parts, output_file)

    if type(con) == type(None):
        con = duckdb.connect(database=':memory:')

    with Manager() as manager:
        d = manager.dict()
        d['table_num'] = 0
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
        pool.map(partial(zip2parquet, my_path=my_path, table_num_dict=d), paths)

    con.execute("CREATE TABLE AllTables(CellValue VARCHAR, TableId UINTEGER, ColumnId USMALLINT, RowId UINTEGER);")
    parquet_files_str = '['
    for zip_path in paths:
        parquet_files_str += "'" + my_path + '/dresden/import/' + zip_path.split('/')[-1].split('.')[0] + '.parquet' + "', "
    parquet_files_str = parquet_files_str[:-2] + ']'
    con.execute("INSERT INTO AllTables SELECT * FROM read_parquet(" + parquet_files_str + ");")
    con.execute("CREATE INDEX token_idx ON AllTables (CellValue);")

    if store_db:
        con.execute("EXPORT DATABASE '" + my_path + "/dresden/db/' (FORMAT PARQUET);")
