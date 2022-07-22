import duckdb
import glob
import pandas as pd
import gzip
import json
import multiprocessing
from functools import partial
import os

#path = '/home/mahdi/DWTC_json'
#my_path = '/home/felix/duckdb'

#path = '/home/neutatz/Software/DataDiscoveryBenchmark/data/dresden'
#my_path = '/home/felix/duckdb'



def zip2parquet(zip_path, my_path=None):
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

def create_db(dir_path):
    my_path = dir_path + '/data'

    con = duckdb.connect(database=':memory:')

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    pool.map(partial(zip2parquet, my_path=my_path), glob.glob(my_path + "/dresden/data/*.json.gz"))

    con.execute("CREATE TABLE AllTables(CellValue VARCHAR, TableId UINTEGER, ColumnId USMALLINT, RowId UINTEGER);")
    con.execute("INSERT INTO AllTables SELECT * FROM read_parquet('" + my_path + "/dresden/import/*.parquet');")
    con.execute("CREATE INDEX token_idx ON AllTables (CellValue);")
    con.execute("EXPORT DATABASE '" + my_path + "/dresden/db/' (FORMAT PARQUET);")
