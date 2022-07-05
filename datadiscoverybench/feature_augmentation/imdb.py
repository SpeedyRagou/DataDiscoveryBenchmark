import pandas as pd
from sklearn.model_selection import train_test_split
from autogluon.tabular import TabularDataset, TabularPredictor
from sklearn.metrics import mean_squared_error, r2_score

#download https://datasets.imdbws.com/title.ratings.tsv.gz
ratings = pd.read_csv('/home/neutatz/Downloads/title.ratings.tsv.gz', compression='gzip', header=0, delimiter='\t')
#download https://datasets.imdbws.com/title.basics.tsv.gz
titles = pd.read_csv('/home/neutatz/Downloads/title.basics.tsv.gz', compression='gzip', header=0, delimiter='\t')

data = ratings.set_index('tconst').join(titles.set_index('tconst'), how='left')
data.drop(['tconst'], axis=1, inplace=True, errors='ignore')

print(data)

y = data['averageRating'].values
data.drop(['averageRating'], axis=1, inplace=True, errors='ignore')
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