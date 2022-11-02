from collections import defaultdict

import duckdb
import pandas as pd
from datadiscoverybench.utils import load_dresden_db
from db_handler import DBHandler
import duckdb


class ExpectationMaximization:
    def __init__(self, delta_epsilon):
        self.delta_epsilon = delta_epsilon
        self.dbHandler = DBHandler()

    def expectation_maximization(self, examples: pd.DataFrame, inp: pd.DataFrame, verbose=True):
        answers = examples.copy()
        tables = None
        finishedQuerying = False
        oldA = None
        delta_score = 0
        lineage_answers = defaultdict(set)
        lineage_tables = defaultdict(set)

        if verbose:
            print("Finished setting up environment")

        while not finishedQuerying or delta_score > self.delta_epsilon:
            new_value = False
            if not finishedQuerying:

                # get direct Transformation candidate
                tables = self.dbHandler.fetch_candidates(answers)


                # get indirect Transformation candidates

                # filter tables by checking functional dependency

                # union tables
                if verbose:
                    print("###############Table Candidates##############")
                    print(tables, '\n')

                for index, table in tables.iterrows():

                    table_id = table[0]

                    rows = self.dbHandler.fetch_table(table_id)



                    input_columns = inp.columns
                    rows_columns = rows.columns
                    if verbose:
                        print(f"For Table {table_id}")
                        print("Checking Rows:")
                        print(rows[rows_columns[table[1]]])
                        print(rows[rows_columns[table[2]]])
                        print('\n')


                    # find all transformations in table
                    possible_candidates = duckdb.query(f"SELECT \"{rows_columns[table[1]]}\", \"{rows_columns[table[2]]}\" " \
                                                       f"FROM rows JOIN inp " \
                                                       f"ON (\"{rows_columns[table[1]]}\"=\"{input_columns[0]}\" )").to_df()

                    if verbose:

                        print("Found possible answers:")
                        print(possible_candidates)
                        print('\n')

                    possible_candidates.columns = answers.columns
                    answers = pd.concat([answers, possible_candidates], axis=0)
                    answers.drop_duplicates(inplace=True)


                    for index, candidate in possible_candidates.iterrows():

                        hash = tuple(candidate)
                        if len(lineage_answers[hash]) == 0:
                            new_value = True
                        lineage_answers[hash].add(table_id)
            if not new_value:
                if verbose:
                    print("Finished")
                finishedQuerying = True

            if verbose:
                print(f"New Answers:\n {answers}\n")

            # Update Table Score
            self.__updateTableScore(answers, tables)
            # Update Answer Score
            self.__updateAnswerScores(answers, tables)
            # calculate delta and compare to delta_epsilon
        print(lineage_answers)

    def __updateTableScore(self, answers, tables):
        pass

    def __updateAnswerScores(self, answers, tables):
        pass
