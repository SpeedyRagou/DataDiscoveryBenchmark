from pathlib import Path

import duckdb
import pandas as pd
import time
from components.expectation_maximization import ExpectationMaximization


class DataXFormer:
    """
    Provides an easy-to-use interface that applies DataXFormer on a given dataset by utilizing dresden web tables or
    a self-defined database
    """

    def __init__(self,
                 max_path: int = 2,
                 delta_epsilon: float = 0.5,
                 tau=2,
                 alpha: float = 0.99,
                 parts: list = None,
                 db_file_path: Path = None,
                 use_table_joiner: bool = False,
                 verbose: bool = False,
                 debug: bool = False
                 ):
        """
        Parameters
        -------------
        :param max_path:
        defines how many iterations the table joiner is allowed to do
        :param delta_epsilon:
        defines threshold that measures if expectation-maximization has converged
        :param tau:
        defines how many examples should be in a table to be seen as a candidate
        :param alpha:
        defines the smoothing factor for table score updating. Should only be a little lower than 1
        :param parts:
        defines which parquet of the dresden web tables is loaded. The integers in this list should be between [0,499].
        If not given, DataXFormer will assume parts = [0]
        :param db_file_path:
        takes a path to a duckDB database file, if user wants to use another database than dresden web tables or the
        user wants to exploit disk for the database. If given parameter parts is ignored
        :param use_table_joiner:
        decides if DataXFormer should use the TableJoiner Component
        :param verbose:
        decides if DataXFormer should print useful information
        :param debug:
        decides if the sql-query is limited to 50 to cut runtime for testing.
        """

        self.max_path = max_path  # Max Length for Joiner
        self.delta_epsilon = delta_epsilon  # Threshold for minimum change in expectation-maximization
        self.tau = tau
        self.alpha = alpha

        if parts is None:
            parts = [0]
        self.parts = parts

        if db_file_path is not None:
            self.db_path = db_file_path.resolve()
        else:
            self.db_path = None

        self.use_table_joiner = use_table_joiner
        self.verbose = verbose
        self.debug = debug

    def run(self, input_frame: pd.DataFrame) -> pd.DataFrame:
        """
        Applies DataXFormer with Expectation-Maximization algorithm. Selects transformations based on score.
        Multi-X values are supported.
        Only 1-1 and n-1 transformations are supported.

        Parameters
        ------------

        :param input_frame: A pandas DataFrame that contains both the examples and query
        values. The query values are identified by having NA in the y column. It should have the form: x1 | x2 | ...
        | xn | y

        :return: Returns a pandas DataFrame that contains the examples, transformed query values and query values where
        no transformation was found
        """
        start_time = time.time()  # get the start time
        # split input into examples and query values
        columns = input_frame.columns
        examples = input_frame.dropna(axis=0, how='any', inplace=False)
        inp = input_frame[input_frame[columns[-1]].isna()]

        # lower input for gittables
        examples = examples.astype(str).applymap(str.lower)
        inp = inp.astype(str).applymap(str.lower)
        input_frame = input_frame.astype(str).applymap(str.lower)

        # call modules (Right now only ExpectationMaximization)
        if self.verbose:
            print("Given Examples:")
            print(examples, "\n\n")
            print("Given Query Values:")
            print(inp, "\n\n")

        mainLoop = ExpectationMaximization(delta_epsilon=self.delta_epsilon,
                                           alpha=self.alpha,
                                           parts=self.parts,
                                           tau=self.tau,
                                           db_path=self.db_path,
                                           use_table_joiner=self.use_table_joiner,
                                           verbose=self.verbose,
                                           debug=self.debug)

        result = mainLoop.expectation_maximization(examples, inp)

        x_columns = list(result.columns[:-2])
        idx = result.groupby(x_columns)[result.columns[-1]].transform(max) == result[result.columns[-1]]
        result = result[idx]

        selection = ""
        on_clause = ""
        for x1, x2 in zip(input_frame.columns[:-1], result.columns[:-2]):
            selection += f"input_frame.\"{x1}\", "
            on_clause += f"input_frame.\"{x1}\" = result.\"{x2}\" AND "
        selection += f"result.\"{result.columns[-2]}\" "
        on_clause = on_clause[:-5]

        query = f"SELECT {selection} " \
                f"FROM input_frame LEFT OUTER JOIN result " \
                f"ON ({on_clause}) "

        joined_result = duckdb.query(query).to_df()

        end_time = time.time()  # get the end time
        elapsed_time = end_time - start_time  # get the execution time

        if self.verbose:
            print("---------------------------------------------")
            print('Total execution time:', elapsed_time, 's')
            print()
            print("---------------------------------------------")
            print("Selected answers and scores")
            print("---------------------------------------------")
            print(result)
            print("---------------------------------------------")
            print()
            print("---------------------------------------------")
            print("Joined result")
            print("---------------------------------------------")
            print(joined_result)
            print("---------------------------------------------")

        return joined_result


if __name__ == '__main__':
    path_to_examples = Path("./data/benchmark/Movie2year.csv").resolve()
    frame = pd.read_csv(path_to_examples, dtype=str, encoding="ISO-8859-1")

    dataxformer = DataXFormer(verbose=True, use_table_joiner=False, debug=False,
                              db_file_path=Path("/home/groupc/gittables_DXF_all.duckdb"))
    transformed_dataframe = dataxformer.run(frame)





