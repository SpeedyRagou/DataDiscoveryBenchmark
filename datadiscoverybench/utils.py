import os
from datadiscoverybench.create_db.create_big_table_from_dresden_parallel import create_db
import urllib.request


dir_path = os.path.dirname(os.path.realpath(__file__))[0:-19]

print(dir_path)

def load_dresden_db(con):

    if not os.path.isdir(dir_path + '/data'):
        os.mkdir(dir_path + '/data')

    if not os.path.isdir(dir_path + '/data/dresden'):
        os.mkdir(dir_path + '/data/dresden')

    if not os.path.isdir(dir_path + '/data/dresden/data'):
        os.mkdir(dir_path + '/data/dresden/data')

    if not os.path.isdir(dir_path + '/data/dresden/db'):
        os.mkdir(dir_path + '/data/dresden/db')

    #print("starting download")
    if not os.path.isfile(dir_path + '/data/dresden/data/' + 'dwtc-000.json.gz'):
        urllib.request.urlretrieve("http://wwwdb.inf.tu-dresden.de/misc/dwtc/data_feb15/dwtc-000.json.gz",
                                   dir_path + '/data/dresden/data/' + 'dwtc-000.json.gz')
    #print("finished download")
    if not os.path.isfile(dir_path + '/data/dresden/db/' + 'alltables.parquet'):
        create_db(dir_path)
    #print("created duckdb")

    con.execute("IMPORT DATABASE '" + dir_path + "/data/dresden/db';")