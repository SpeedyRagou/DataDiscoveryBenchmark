from os import listdir
from os.path import isfile, join
from pathlib import Path

import pandas as pd
from sklearn.metrics import precision_recall_fscore_support
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
    #df = df.head(20)
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

    db_files = [Path("/home/groupc/gittables.duckdb")]
    f1 = 0
    recall = 0
    precision = 0
    iteration = 1
    max_iteration = len(db_files) * len(files)
    f1_dataframe = pd.DataFrame(columns=["File", "Precision", "Recall", "F1-Score", "Time", "Average Iteration Time", "Iteration", "Found Answers"])
    print(f1_dataframe)
    for db_file in db_files:
        for f in files:
            print(f"Experiment {iteration}/{max_iteration}: {f}")
            iteration += 1
            try:
                p = Path(path_benchmark + f).resolve()
                df, ground_truth = create_examples_csv(p)

                dataxformer = DataXFormer(verbose=False, use_table_joiner=False, debug=False,
                                        db_file_path=db_file)
                transformed_df = dataxformer.run(df)

                stem = Path(path_results + f).resolve().stem
                transformed_df.to_csv(Path(f"{path_results}{stem}_{db_file.stem}_result.csv"), index=False)

                input_length = df.dropna(axis=0, how='any', inplace=False).shape[0]
                result_length = transformed_df.dropna(axis=0, how='any', inplace=False).shape[0]
                # DataXFormer has to return all examples and query values for the following to work

                precision_file, recall_file, f1_file, support = precision_recall_fscore_support(ground_truth.iloc[:, -1].to_numpy().astype(str), transformed_df.iloc[:, -1].to_numpy().astype(str), average='micro')

                f1 += f1_file
                recall += recall_file
                precision += precision_file

                f1_dataframe.loc[len(f1_dataframe.index)] = [f, precision_file, recall_file, f1_file, dataxformer.elapsed_time, sum(dataxformer.elapsed_iteration_times)/len(dataxformer.elapsed_iteration_times), len(dataxformer.elapsed_iteration_times), result_length - input_length]
                # Until then ...
                # f1 += 0.85
                print(f1_dataframe)

            except:
                print("An error happend")
                continue
        f1 /= len(files)
        recall /= len(files)
        precision /= len(files)

        print(f"Average F1-Score for {db_file.stem}:", f1)
        print(f"Average Recall for {db_file.stem}:", recall)
        print(f"Average Precision for {db_file.stem}:", precision_file)

        f1_dataframe.to_csv(Path(f"data/f1_results_{db_file.stem}.csv"), index=False)
