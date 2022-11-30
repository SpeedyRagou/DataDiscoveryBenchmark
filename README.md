# Data Discovery Benchmark

You can setup this project on Linux, Windows, or MacOS. Additionally, you can also use Google Colab.

Here is an example about using the project in Google Colab: [Feature Augmentation with Webtables](https://colab.research.google.com/drive/1wgJXLl8rnsxtCBcWWPugqlxfw6CQpVg_?usp=sharing)


This is how to setup the project on your machine:
```
conda create -n Bench python=3.8
conda activate Bench
git clone https://github.com/LUH-DBS/DataDiscoveryBenchmark.git
cd DataDiscoveryBenchmark
git pull origin main
python -m pip install .
```

If `python -m pip install .` does not work, try this command:
```
python -m pip install --use-feature=in-tree-build .
```

You can run the feature augmentation task with:
```
python datadiscoverybench/feature_augmentation/augmentation.py
```

On Mac OS, you need to run the following command additionally:
```
brew install libomp
```

# DataXFormer
In this repository an implementation of DataXFormer can be found
## How to use DataXFormer
First we load a DataFrame. All examples should have x-values and y-values. The query values should have x-values and no y-values (NaN).
```
path_to_examples = Path("./Examples/benchmark/CountryToLanguage.csv").resolve()
frame = pd.read_csv(path_to_examples, dtype=str, encoding="ISO-8859-1") # Load Dataframe

# Tokenize input in the same way the database is
```

After that we create the DataXFormer object and call it with the loaded DataFrame.

```
dataxformer = DataXFormer(verbose=True, use_table_joiner=False)
transformed_dataframe = dataxformer.run(frame)
```

## Parameters
The DataXFormer has multiple options that can used:

```
 :param: max_path
```
It defines how many iterations the Table Joiner is at maximum allowed to do. Increasing this will increase runtime, but not necessarily improve the transformations.
#
```
:param delta_epsilon:
```
This threshold directly controls when the expectation-maximization says it has converged. It is compared to the amount of change that happens in each Iteration to the answer scores.
#
```
:param tau:
```
Directly controls how many examples should be in a table to be counted as useful. This can change the quality and amount of transformations found.
#
```
:param alpha:
```
Defines the smoothing factor for table score updating. Should only be a little lower than 1. Should normally not be changed.
#
```
:param parts:
```
Normally this implementation loads parquets of the dresden web-tables to use as its in-memory database (not tokenized). This parameter controls how many and which parquets should be loaded. 
The List should only contain numbers between 0 and 500. If a large amount of parquets is loaded, a large amount of RAM will be used.
#
```
:param db_file_path:
```
This parameter can be used to make the DataXFormer use a customized database. The database should be loadable by duckdb. If this parameter is set the 'parts' parameter is ignored.
The database should contain one table called 'AllTables' with 4 columns named 'CellValue', 'TableId', 'ColumnId' and 'RowId'
#
```
:param use_table_joiner:
```
If activated this will allow DataXFormer to use the TableJoiner Component. This will drastically increase the runtime and increase RAM usage.
The TableJoiner Component is untested and in an experimental state, so usage is not recommended
#
```
:param verbose:
```
This parameter allows the DataXFormer to print out useful information, but it will also be cluttering the terminal.
#
```
:param debug:
```
This will set DataXFormer into debug mode, which will decrease runtime, but limit the Tables found in the database, so the result should not be used.