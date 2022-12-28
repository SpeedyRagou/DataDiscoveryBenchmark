from os import listdir
from os.path import isfile, join
from pathlib import Path

import pandas as pd
from sklearn.metrics import f1_score
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
    df = df.astype(str).applymap(str.lower)

    # Cap the size of the csv to 20 rows and save
    df = df.head(20)
    df.to_csv("./data/benchmark/{0}.csv".format(filename), index=False)

    ground_truth = df.copy(deep=True)

    # Keep the first 5 transformations as examples and delete the y-values of the rest
    df.iloc[:, -1] = df.iloc[:5, -1]
    df.to_csv("./data/examples/{0}.csv".format(filename), index=False)

    return df, ground_truth


if __name__ == "__main__":

    path_benchmark = "data/benchmark/"
    path_results = "data/results/"

    files = [f for f in listdir(path_benchmark) if isfile(join(path_benchmark, f))]

    db_files = [Path("/home/groupc/gittables.duckdb"), Path("/home/groupc/dwtc.duckdb")]
    f1 = 0

    f1_dataframe = pd.DataFrame(columns=["File", "F1-Score", "Time", "Average Iteration Time"])
    print(f1_dataframe)
    for db_file in db_files:
        for f in files:
            p = Path(path_benchmark + f).resolve()
            df, ground_truth = create_examples_csv(p)

            dataxformer = DataXFormer(verbose=False, use_table_joiner=False, debug=False,
                                      db_file_path=db_file)
            transformed_df = dataxformer.run(df)

            stem = Path(path_results + f).resolve().stem
            transformed_df.to_csv(Path(f"{path_results}{stem}_{db_file.stem}_result.csv"), index=False)

            # DataXFormer has to return all examples and query values for the following to work

            f1_file = f1_score(ground_truth.iloc[:, -1].to_numpy(), transformed_df.iloc[:, -2].to_numpy(), average='micro')
            f1 += f1_file

            f1_dataframe.loc[len(f1_dataframe.index)] = [f, f1_file, dataxformer.elapsed_time, sum(dataxformer.elapsed_iteration_times)/len(dataxformer.elapsed_iteration_times)]
            # Until then ...
            # f1 += 0.85
            print(f1_dataframe)
        f1 /= len(files)
        print(f"Average F1-Score for {db_file.stem}:", f1)

        f1_dataframe.to_csv(Path(f"data/f1_results_{db_file.stem}.csv"), index=False)
