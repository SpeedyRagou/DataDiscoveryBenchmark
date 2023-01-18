from os import listdir
from os.path import isfile, join
from pathlib import Path

path_to_examples = Path("./data/examples").resolve()

path_to_results = Path("./data/results").resolve()

path_examples = "data/examples/"
path_results = "data/results/"

files = [f for f in listdir(path_examples) if isfile(join(path_examples, f))]
files_res = [f for f in listdir(path_results) if isfile(join(path_results, f))]

print(list(zip(files, files_res)))
