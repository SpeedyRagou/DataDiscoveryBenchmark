from pathlib import Path
import duckdb
import numpy as np
from datadiscoverybench.utils import load_dresden_db

if __name__ == '__main__':
    db_path = Path("/home/groupc/dwtc.duckdb").resolve()
    con = duckdb.connect(database=str(db_path))
    load_dresden_db(con, parts=np.arange(500))


