import os
from datadiscoverybench.create_db.create_big_table_from_dresden_parallel import create_db as create_dresden_db
from datadiscoverybench.create_db.create_big_table_from_gittables_parquet_parallel import create_db as create_gittables_db
import urllib.request
import pickle

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

dir_path = os.path.dirname(os.path.realpath(__file__))

def setup_db(name):
    if not os.path.isdir(dir_path + '/data'):
        os.mkdir(dir_path + '/data')

    if not os.path.isdir(dir_path + '/data/' + name):
        os.mkdir(dir_path + '/data/' + name)

    if not os.path.isdir(dir_path + '/data/' + name + '/data'):
        os.mkdir(dir_path + '/data/' + name + '/data')

    if not os.path.isdir(dir_path + '/data/' + name + '/db'):
        os.mkdir(dir_path + '/data/' + name + '/db')

    if not os.path.isdir(dir_path + '/data/' + name + '/import'):
        os.mkdir(dir_path + '/data/' + name + '/import')


def load_dresden_db(con, parts=[0], store_db=True):
    db_name = 'dresden'
    setup_db(db_name)

    #print("starting download")
    for p in parts:
        formatted_id = str(p).zfill(3)
        if not os.path.isfile(dir_path + '/data/' + db_name + '/data/' + 'dwtc-' + formatted_id + '.json.gz'):
            urllib.request.urlretrieve("http://wwwdb.inf.tu-dresden.de/misc/dwtc/data_feb15/dwtc-" + formatted_id + ".json.gz",
                                       dir_path + '/data/' + db_name + '/data/' + 'dwtc-' + formatted_id + '.json.gz')
    #print("finished download")

    if os.path.isfile(dir_path + '/data/' + db_name + '/db/' + 'alltables.parquet'):
        with open(dir_path + '/data/' + db_name + '/db/' + 'parts.pickle', "rb") as input_file:
            parts_old = pickle.load(input_file)
            if set(parts_old) != set(parts):
                create_dresden_db(dir_path, parts, con=con, store_db=store_db)
            else:
                con.execute("IMPORT DATABASE '" + dir_path + "/data/" + db_name + "/db';")
    else:
        create_dresden_db(dir_path, parts, con=con, store_db=store_db)
    #print("created duckdb")

# find single files here: https://zenodo.org/record/6517052
def load_git_tables_db(con, parts=['allegro_con_spirito_tables_licensed'], store_db=True):
    db_name = 'gittables'
    setup_db(db_name)

    # print("starting download")
    for p in parts:
        if not os.path.isfile(dir_path + '/data/' + db_name + '/data/' + p + '.zip'):
            urllib.request.urlretrieve(
                "https://zenodo.org/record/6517052/files/" + p + ".zip",
                dir_path + '/data/' + db_name + '/data/' + p + '.zip')
    # print("finished download")

    if os.path.isfile(dir_path + '/data/' + db_name + '/db/' + 'alltables.parquet'):
        with open(dir_path + '/data/' + db_name + '/db/' + 'parts.pickle', "rb") as input_file:
            parts_old = pickle.load(input_file)
            if set(parts_old) != set(parts):
                create_gittables_db(dir_path, parts, con=con, store_db=store_db)
            else:
                con.execute("IMPORT DATABASE '" + dir_path + "/data/" + db_name + "/db';")
    else:
        create_gittables_db(dir_path, parts, con=con, store_db=store_db)
    # print("created duckdb")
