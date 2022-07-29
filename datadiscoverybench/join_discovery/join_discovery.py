import duckdb
import pandas as pd
from datadiscoverybench.utils import load_git_tables_db
from datadiscoverybench.feature_augmentation.imdb.IMDB_Dataset import IMDB
from datadiscoverybench.utils import load_dresden_db

def find_joinable(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, join_column_name: str) -> list:
    result = con.execute('''SELECT COUNT(*) as ct, TableId FROM 
                                (SELECT COUNT (*) as count, TableId, CellValue 
                                    FROM ALLTables 
                                    WHERE CellValue IN 
                                        (SELECT ''' + join_column_name + ''' FROM df)
                                    GROUP BY TableId, CellValue 
                                    )
                            GROUP BY TableId
                            ORDER BY ct DESC
                            FETCH FIRST 20 ROWS ONLY;''' ).fetch_df()

    return result['TableId'].values

def main():
    dataset = IMDB(number_rows=1000)
    data = dataset.get_df() 

    join_col_name = 'primaryTitle'

    con = duckdb.connect(database=':memory:')
    load_dresden_db(con)
    # load_git_tables_db(con)

    result = find_joinable(con, data, join_col_name)

if __name__ == "__main__":
    main()