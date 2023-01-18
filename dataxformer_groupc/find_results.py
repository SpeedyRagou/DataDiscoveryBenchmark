from os import listdir
from os.path import isfile, join
from pathlib import Path

import pandas as pd

path_to_examples = Path("./data/examples").resolve()

path_to_results = Path("./data/results").resolve()
path_to_benchmark = Path("./data/benchmark").resolve()

path_examples = "data/examples/"
path_results = "data/results/"
path_to_bench = "data/benchmark/"
files = [f for f in listdir(path_examples) if isfile(join(path_examples, f))]
files_res = [f for f in listdir(path_results) if isfile(join(path_results, f))]
pairs = []

for f in files:
    for f_res in files_res:
        if f[:len(f) - 4] in f_res:
            pairs.append((f, f_res))
            break

print(pairs)

for pair in pairs:
    path_to_examples_absolute = path_to_examples.joinpath(pair[0]).resolve()
    path_to_results_absolute = path_to_results.joinpath(pair[1]).resolve()
    try:
        res_df = pd.read_csv(path_to_results_absolute, dtype=str, encoding="ISO-8859-1")
        ex_df = pd.read_csv(path_to_examples_absolute, dtype=str, encoding="ISO-8859-1")
    except Exception:
        continue

    if res_df.shape[0] == ex_df.shape[0]:
        path_to_benchmark_absolute = path_to_benchmark.joinpath(pair[0]).resolve()
        bench_df = pd.read_csv(path_to_benchmark_absolute, dtype=str, encoding="ISO-8859-1")

        ex_df = ex_df.dropna()
        res_df_r = res_df.dropna()
        print(f"------ For pair {pair} ---------")
        print(f"{ex_df.shape[0]} : {res_df_r.shape[0]}")
        if ex_df.shape[0] < res_df_r.shape[0]:
            print(f"------ For pair {pair} ---------")
            print(f"{ex_df.shape[0]} : {res_df_r.shape[0]}")
            res_df.sort_values(res_df.columns[0], axis=0, inplace=True)
            print(res_df)
            bench_df.sort_values(bench_df.columns[0], axis=0, inplace=True)
            print(bench_df)

