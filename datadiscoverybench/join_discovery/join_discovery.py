''' For join discovery, we need an input table, one of the columns of the input table, and a lake from which we can find joinable tables. 
    The goal is to find joinable tables with the input table on the specified input column. 
'''

import duckdb
import pandas as pd
from datadiscoverybench.utils import load_git_tables_db
from datadiscoverybench.feature_augmentation.imdb.IMDB_Dataset import IMDB
from datadiscoverybench.utils import load_dresden_db

# This method finds top-20 joinable tables from the specified lake. 
def find_joinable(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, join_column_name: str) -> list:
    result = con.execute('''SELECT COUNT(*) as ct, TableId FROM 
                                (SELECT COUNT (*) as count, ColumnId, TableId, CellValue -- > Selecting all data cells from all tables of the lake that their values are inside the list of cell values extracted below
                                    FROM ALLTables 
                                    WHERE CellValue IN 
                                        (SELECT ''' + join_column_name + ''' FROM df) -- > select all values of the input column inside the input table
                                    GROUP BY TableId, ColumnId, CellValue -- > To get disctinct values per column of a table
                                    )
                            GROUP BY TableId 
                            ORDER BY ct DESC -- > order the tables based on the number of joinable values
                            FETCH FIRST 20 ROWS ONLY; -- > fetch top-20 tables with the most number of shared distinct values with the input table''' ).fetch_df()

    return result['TableId'].values

def main():

    # input dataset 
    dataset = IMDB(number_rows=1000)
    data = dataset.get_df() 

    # the column on which we want to do the join
    join_col_name = 'primaryTitle'

    # connection to the duckdb (database) and loading the desiered lake for finding joinable tables
    con = duckdb.connect(database=':memory:')
    load_dresden_db(con)
    # load_git_tables_db(con)

    result = find_joinable(con, data, join_col_name)

if __name__ == "__main__":
    main()