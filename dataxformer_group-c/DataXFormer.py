from pathlib import Path

import pandas as pd
from expectation_maximization import ExpectationMaximization


class DataXFormer:
    def __init__(self, max_path=2, delta_epsilon=0.5):
        self.max_path = max_path  # Max Length for Joiner
        self.delta_epsilon = delta_epsilon  # Threshold for minimum change in expectation-maximization

    def run(self, path_to_input_csv: Path) -> pd.DataFrame:
        # load input csv
        input_frame = pd.read_csv(path_to_input_csv, dtype=str)
        columns = input_frame.columns
        examples = input_frame.dropna(axis=0, how='any', inplace=False)
        inp = input_frame[input_frame[columns[-1]].isna()]
        # tokenize input csv
        # use nltk package for this

        # stem input csv
        # use nltk package for this

        # send to main loop
        print("Given Examples:")
        print(examples, "\n\n")
        print("Given Query Values:")
        print(inp, "\n\n")
        mainLoop = ExpectationMaximization(self.delta_epsilon)
        mainLoop.expectation_maximization(examples, inp)



if __name__ == '__main__':
    path_to_examples = Path("../dataxformer_group-c/Examples/benchmark/AsciiToUnicode.csv").resolve()
    dataxformer = DataXFormer()
    dataxformer.run(path_to_examples)
