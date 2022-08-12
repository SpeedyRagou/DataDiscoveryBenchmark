import duckdb
from datadiscoverybench.utils import dir_path
import time

def main():
    con = duckdb.connect(database=dir_path + '/disk_db/' + 'db1.txt')
    start = time.time()
    print(con.execute("SELECT count(*) FROM AllTables").fetch_df())
    print('time: ' + str(time.time() - start))

if __name__ == "__main__":
    main()