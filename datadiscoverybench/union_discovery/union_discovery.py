import pandas as pd
from sklearn.model_selection import train_test_split
from autogluon.tabular import TabularDataset, TabularPredictor
from sklearn.metrics import mean_squared_error, r2_score
import duckdb
import time
from datadiscoverybench.utils import load_dresden_db
from datadiscoverybench.utils import load_git_tables_db
from datadiscoverybench.feature_augmentation.imdb.IMDB_Dataset import IMDB


def discover(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, column_name: str, id_column_name: str) -> pd.DataFrame:
    df.originalTitle = [str(x).replace('\'','') for x in df.originalTitle.values]

    first_num_dist_vals = len(set(df.originalTitle.values))
    first_col = '\',\''.join(list(set(df.originalTitle.values)))
    first_query = con.execute('''
                                    SELECT TableId, ColumnId, COUNT(*) AS overlap FROM AllTables 
                                    WHERE CellValue IN (' ''' + first_col +''' ')
                                    GROUP BY TableId, ColumnId
                                    ORDER BY overlap DESC;
    ''').fetch_df()
    first_query['ID1'] = first_query["TableId"] + '___' + first_query["ColumnId"].astype(str)
    first_query.drop(['TableId', 'ColumnId'], inplace=True, axis=1)
    first_query.overlap = first_query.overlap/first_num_dist_vals

    second_num_dist_vals = len(set(df.genres.values))
    second_col = '\',\''.join([str(x) for x in set(df.genres.values)])
    second_query = con.execute('''
                                        SELECT TableId, ColumnId, COUNT(*) AS overlap FROM AllTables 
                                        WHERE CellValue IN (' ''' + second_col + ''' ')
                                        GROUP BY TableId, ColumnId
                                        ORDER BY overlap DESC;
        ''').fetch_df()
    second_query['ID2'] = second_query["TableId"] + '___' + second_query["ColumnId"].astype(str)
    second_query.drop(['TableId', 'ColumnId'], inplace=True, axis=1)
    second_query.overlap = second_query.overlap / second_num_dist_vals

    third_num_dist_vals = len(set(df.startYear.values))
    third_col = '\',\''.join([str(x) for x in set(df.startYear.values)])
    third_query = con.execute('''
                                        SELECT TableId, ColumnId, COUNT(*) AS overlap FROM AllTables 
                                        WHERE CellValue IN (' ''' + third_col + ''' ')
                                        GROUP BY TableId, ColumnId
                                        ORDER BY overlap DESC;
        ''').fetch_df()
    third_query['ID3'] = third_query["TableId"] + '___' + third_query["ColumnId"].astype(str)
    third_query.drop(['TableId', 'ColumnId'], inplace=True, axis=1)
    third_query.overlap = third_query.overlap / third_num_dist_vals

    joint_df = pd.merge(pd.merge(first_query, second_query, left_on='ID1', right_on='ID2', how='outer'), third_query, left_on='ID1', right_on='ID3')
    joint_df['overlap_x'] = joint_df['overlap_x'].fillna(0)
    joint_df['overlap_y'] = joint_df['overlap_y'].fillna(0)
    joint_df['overlap'] = joint_df['overlap'].fillna(0)
    joint_df['unionability_score'] = joint_df.overlap_x + joint_df.overlap_y + joint_df.overlap
    joint_df.sort_values(['unionability_score'], inplace=True, ascending=False)
    pd.set_option('display.max_columns', None)
    return joint_df[['unionability_score', 'ID1', 'ID2', 'ID3']]


dataset = IMDB()
data = dataset.get_df()

data.to_csv('joint_data.csv', index=False)

con = duckdb.connect(database=':memory:')
# load_dresden_db(con)
load_git_tables_db(con)

print(con.execute('select count(*) from AllTables').fetch_df())

start_discover_time = time.time()
data = discover(con, data, column_name='originalTitle', id_column_name=dataset.id_column_name)
total_discover_time = time.time()-start_discover_time
print(data)
print(total_discover_time)


