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
        score_answers = {}

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

                    rows = self.dbHandler.fetch_table_columns(table)



                    input_columns = list(inp.columns)
                    rows_columns = list(rows.columns)
                    if verbose:
                        print(f"For Table {table_id}")
                        print("Checking Rows:")
                        print(rows)
                        print(rows_columns)
                        print(input_columns)
                        print('\n')

                    select_statement = ""
                    for col_row in rows_columns:
                        select_statement += f" \"{col_row}\","
                    select_statement = select_statement[:len(select_statement) - 1]

                    on_statement = ""
                    for col_row, col_in in zip(rows_columns[:len(rows_columns)-1], input_columns[:len(input_columns)-1]):
                        on_statement += f"\"{col_row}\"=\"{col_in}\" AND "
                    on_statement = on_statement[: len(on_statement) - 5]
                    # find all transformations in table

                    transformation_query = f"SELECT {select_statement} " \
                            f"FROM rows JOIN inp " \
                            f"ON ({on_statement})"

                    possible_candidates = duckdb.query(transformation_query).to_df()

                    if verbose:
                        print("Found possible answers:")
                        print(transformation_query)
                        print(possible_candidates)
                        print('\n')

                    possible_candidates.columns = answers.columns
                    answers = pd.concat([answers, possible_candidates], axis=0)
                    answers.drop_duplicates(inplace=True)



                    for index, candidate in possible_candidates.iterrows():

                        hash = tuple(candidate)
                        if len(lineage_answers[hash]) == 0:
                            new_value = True
                        lineage_answers[hash].add(tuple(table))
                        lineage_tables[tuple(table)].add(hash)

            if not new_value:
                if verbose:
                    print("Finished")
                finishedQuerying = True

            if verbose:
                print(f"New Answers:\n {answers}\n")

            # Update Table Score
            self.__updateTableScore(answers, tables, lineage_tables)
            # Update Answer Score
            self.__updateAnswerScores(answers, tables)
            # calculate delta and compare to delta_epsilon
        print(lineage_answers)
        print(lineage_tables)

    def __updateTableScore(self, answers, tables: pd.DataFrame, lineage_tables):
        for index, table in tables.iterrows():
            good = 0
            bad = 0
            total = 0
            coveredX = lineage_tables[tuple(table)]

        pass

    def __updateAnswerScores(self, answers, tables):
        pass
