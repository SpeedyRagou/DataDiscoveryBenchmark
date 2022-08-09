import duckdb
from datadiscoverybench.utils import load_dresden_db
from datadiscoverybench.utils import load_git_tables_db
import os
from datadiscoverybench.utils import dir_path


def main():
    if not os.path.isdir(dir_path + '/disk_db/'):
        os.mkdir(dir_path + '/disk_db/')

    #con = duckdb.connect(database=':memory:')
    con = duckdb.connect(database=dir_path + '/disk_db/' + 'db1.txt')
    #print(con.execute("DROP TABLE AllTables").fetch_df())
    #load_dresden_db(con, parts=list(range(2)), store_db=False)
    load_dresden_db(con, parts=list(range(500)), store_db=False)

    print(con.execute("SELECT count(*) FROM AllTables").fetch_df())

if __name__ == "__main__":
    main()