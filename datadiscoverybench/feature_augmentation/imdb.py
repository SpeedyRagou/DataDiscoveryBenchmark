import pandas as pd
from sklearn.model_selection import train_test_split
from autogluon.tabular import TabularDataset, TabularPredictor
from sklearn.metrics import mean_squared_error, r2_score
import duckdb
import time

def augment_titles(titles: pd.DataFrame, column_name: str) -> pd.DataFrame:
    con = duckdb.connect(database=':memory:')
    con.execute("IMPORT DATABASE '/home/neutatz/Software/DataDiscoveryBenchmark/data/new_git_parallel/gittables/db/';")

    result = con.execute("SELECT titles.*, TEMPT1.Feature FROM titles LEFT JOIN (SELECT titles.originalTitle as originalTitle, Count(*) AS Feature FROM titles LEFT JOIN AllTables ON titles.originalTitle=AllTables.CellValue GROUP BY titles.originalTitle) AS TEMPT1 ON titles.originalTitle=TEMPT1.originalTitle ORDER BY titles.tconst ASC;").fetch_df()

    return result


#download https://datasets.imdbws.com/title.ratings.tsv.gz
ratings = pd.read_csv('/home/neutatz/Downloads/title.ratings.tsv.gz', compression='gzip', header=0, delimiter='\t')
#download https://datasets.imdbws.com/title.basics.tsv.gz
titles = pd.read_csv('/home/neutatz/Downloads/title.basics.tsv.gz', compression='gzip', header=0, delimiter='\t')


start_join = time.time()

con = duckdb.connect(database=':memory:')
data = con.execute("SELECT * FROM ratings LEFT JOIN titles ON titles.tconst=ratings.tconst ORDER BY titles.tconst ASC;").fetch_df()
data.drop(['tconst_2'], axis=1, inplace=True, errors='ignore')

print('time: ' + str(time.time() - start_join))


print(data.columns)
print(len(data.columns))

y = data['averageRating'].values
data.drop(['averageRating'], axis=1, inplace=True, errors='ignore')

#augment
print('start querying')
print(len(data))
augmentation_time_start = time.time()
#data = augment_titles(data, 'originalTitle')
augmentation_time = time.time() - augmentation_time_start
print(len(data))
print(data.columns)

data.drop(['tconst'], axis=1, inplace=True, errors='ignore')

print(data.head())


X = data.values

X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=10000, random_state=42)

df = pd.DataFrame(data=X_train)
label = 'class'
df[label] = y_train
my_data_train = TabularDataset(data=df)

df_test = pd.DataFrame(data=X_test)
my_data_test = TabularDataset(data=df_test)

predictor = TabularPredictor(label=label, problem_type='regression').fit(train_data=my_data_train, time_limit=5*60)
predictions = predictor.predict(my_data_test)

print('r2 score: ' + str(r2_score(y_test, predictions)))
print('mean_squared_error: ' + str(mean_squared_error(y_test, predictions, squared=False)))
print('augmentation time: ' + str(augmentation_time))