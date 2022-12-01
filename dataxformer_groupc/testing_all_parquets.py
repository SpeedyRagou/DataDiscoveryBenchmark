from pathlib import Path

from DataXFormer import DataXFormer
import numpy as np

if __name__ == '__main__':

    path_to_examples = Path("/Examples/benchmark/CountryToLanguage.csv").resolve()
    parquets = np.array([i for i in range(500)])
    parquets = parquets.reshape(10, 50)
    for i in range(parquets.shape[0]):

        print(f"\nParquet Package {i}\n")

        dataxformer = DataXFormer(parts=parquets[i].tolist())
        dataxformer.run(path_to_examples)
