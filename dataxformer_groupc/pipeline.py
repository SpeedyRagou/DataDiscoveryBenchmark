from os import listdir
from os.path import isfile, join
from pathlib import Path

import pandas as pd
from sklearn.metrics import precision_score, recall_score
from DataXFormer import DataXFormer


def create_examples_csv(path):
    """
    Given a csv file of transformations, this function creates another csv file in the same directory,
    which contains examples and query values.
    Temporary Stuff:
    - Currently, examples are NOT picked at random to make results comparable.
      So, the first 5 transformations are picked as examples.
    - Currently, the size of the csv is capped to the first 20 rows.

    :param path: Path to csv file, that contains transformations

    :return: Additionally to saving the new dataframe, return it
    """

    # Resolve path, store filename, create dataframe
    path_resolved = Path(path).resolve()
    filename = Path(path_resolved).stem
    benchmark_df = pd.read_csv(path_resolved, dtype=str, encoding="ISO-8859-1")

    # Cap the size of the csv to 20 rows and save
    benchmark_df = benchmark_df.head(20)
    benchmark_df.to_csv("./data/benchmark/{0}.csv".format(filename), index=False)

    # Keep the first 5 transformations as examples and delete the y-values of the rest
    examples_df = benchmark_df.copy(deep=True)
    examples_df.iloc[:, -1] = examples_df.iloc[:5, -1]
    examples_df.to_csv("./data/examples/{0}.csv".format(filename), index=False)

    return benchmark_df, examples_df


if __name__ == "__main__":

    path_benchmark = "data/benchmark/"

    files = [f for f in listdir(path_benchmark) if isfile(join(path_benchmark, f))]

    prec = 0
    recall = 0

    for f in files:
        p = Path(path_benchmark + f).resolve()
        benchmark_df, examples_df = create_examples_csv(p)

        dataxformer = DataXFormer(verbose=True, use_table_joiner=False, debug=False)#,
                                  #db_file_path=Path("/home/groupc/gittables_DXF_all.duckdb"))
        transformed_df = dataxformer.run(examples_df)

        # Precision and Recall
        prec += precision_score(benchmark_df.iloc[:, -1].astype(str).to_numpy(),
                                transformed_df.iloc[:, -1].astype(str).to_numpy(),
                                average=None, zero_division=0)
        recall += recall_score(benchmark_df.iloc[:, -1].astype(str).to_numpy(),
                               transformed_df.iloc[:, -1].astype(str).to_numpy(),
                               average=None, zero_division=0)

    prec /= len(files)
    recall /= len(files)
    print("Average Precision:", prec)
    print("Average Recall:", recall)
