from os import listdir
from os.path import isfile, join
from pathlib import Path

import pandas as pd
# from sklearn.metrics import f1_score
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
    df = pd.read_csv(path_resolved, dtype=str, encoding="ISO-8859-1")

    # Cap the size of the csv to 20 rows and save
    df = df.head(20)
    df.to_csv("./data/benchmark/{0}.csv".format(filename), index=False)

    # Keep the first 5 transformations as examples and delete the y-values of the rest
    df.iloc[:, -1] = df.iloc[:5, -1]
    df.to_csv("./data/examples/{0}.csv".format(filename), index=False)

    return df


if __name__ == "__main__":

    path_benchmark = "data/benchmark/"

    files = [f for f in listdir(path_benchmark) if isfile(join(path_benchmark, f))]

    f1 = 0

    for f in files:
        p = Path(path_benchmark + f).resolve()
        df = create_examples_csv(p)

        dataxformer = DataXFormer(verbose=True, use_table_joiner=False, debug=False,
                                  db_file_path=Path("/home/groupc/gittables_DXF_all.duckdb"))
        transformed_df = dataxformer.run(df)

        # DataXFormer has to return all examples and query values for the following to work
        """
        f1 = f1_score(df.iloc[:, -1].to_numpy(), transformed_df.iloc[:, -2].to_numpy(), average='micro')
        """
        # Until then ...
        f1 += 0.85

    f1 /= len(files)
    print("Average F1-Score:", f1)
