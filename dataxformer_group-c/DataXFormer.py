from pathlib import Path

import pandas as pd
from expectation_maximization import ExpectationMaximization
import re

def get_cleaned_text(text):
    # if text is None or len(str(text)) == 1:
    #     return ''
    stopwords = ['a', 'the', 'of', 'on', 'in', 'an', 'and', 'is', 'at', 'are', 'as', 'be', 'but', 'by', 'for', 'it',
                 'no', 'not', 'or', 'such', 'that', 'their', 'there', 'these', 'to', 'was', 'with', 'they', 'will',
                 'v', 've', 'd']

    cleaned = re.sub('[\W_]+', ' ', str(text).encode('ascii', 'ignore').decode('ascii')).lower()
    feature_one = re.sub(' +', ' ', cleaned).strip()
    punct = [',', '.', '!', ';', ':', '?', "'", '"', '\t', '\n']

    for x in stopwords:
        feature_one = feature_one.replace(' {} '.format(x), ' ')
        if feature_one.startswith('{} '.format(x)):
            feature_one = feature_one[len('{} '.format(x)):]
        if feature_one.endswith(' {}'.format(x)):
            feature_one = feature_one[:-len(' {}'.format(x))]

    for x in punct:
        feature_one = feature_one.replace('{}'.format(x), ' ')
    return feature_one



class DataXFormer:
    def __init__(self, max_path=2, delta_epsilon=0.5, tau=2, parts: list =None):
        if parts is None:
            parts = [0]
        self.max_path = max_path  # Max Length for Joiner
        self.delta_epsilon = delta_epsilon  # Threshold for minimum change in expectation-maximization
        self.parts = parts
        self.tau = tau

    def run(self, path_to_input_csv: Path) -> pd.DataFrame:
        # load input csv
        input_frame = pd.read_csv(path_to_input_csv, dtype=str, encoding="ISO-8859-1")
        columns = input_frame.columns
        examples = input_frame.dropna(axis=0, how='any', inplace=False)
        inp = input_frame[input_frame[columns[-1]].isna()]

        # tokenize input csv
        #examples = examples.applymap(get_cleaned_text)
        #inp = inp.applymap(get_cleaned_text)

        # lower input for gittables
        examples = examples.astype(str).applymap(str.lower)
        inp = inp.astype(str).applymap(str.lower)

        # use nltk package for this


        # send to main loop
        print("Given Examples:")
        print(examples, "\n\n")
        print("Given Query Values:")
        print(inp, "\n\n")
        mainLoop = ExpectationMaximization(self.delta_epsilon, 0.99, debug=False, parts=self.parts, tau=self.tau)
        result = mainLoop.expectation_maximization(examples, inp)

        query = f"SELECT *, MAX(\"{result.columns[-1]}\")" \
                f"FROM result " \
                f"GROUP BY"

        idx = result.groupby(result.columns[:-2])[result.columns[-1]].transform(max) == result.columns[-1]

        print()
        print("---------------------------------------------")
        print("Final result")
        print("---------------------------------------------")
        print(result[idx])
        print("---------------------------------------------")


if __name__ == '__main__':
    path_to_examples = Path("../dataxformer_group-c/Examples/CountryToLanguage.csv").resolve()
    dataxformer = DataXFormer(tau=2)
    dataxformer.run(path_to_examples)
