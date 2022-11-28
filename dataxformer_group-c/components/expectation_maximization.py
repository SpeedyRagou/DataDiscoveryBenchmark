from pathlib import Path

import numpy as np
import pandas as pd

from .db_handler import DBHandler
from .table_filter import TableFilter
import duckdb

class ExpectationMaximization:
    """
    Implements the Expectation-Maximization algorithm as a component for DataXFormer
    """
    def __init__(
            self,
            delta_epsilon: float = 0.5,
            alpha: float = 0.99,
            tau: int = 2,
            max_path: int = 2,
            parts: list = None,
            db_path: Path = None,
            use_table_joiner: bool = False,
            verbose: bool = True,
            debug: bool = True

    ):
        """
        Parameters:
        --------------
        :param delta_epsilon:
        defines threshold that measures if expectation-maximization has converged
        :param alpha:
        defines the smoothing factor for table score updating. Should only be a little lower than 1
        :param tau:
        defines how many examples should be in a table to be seen as a candidate
        :param max_path:
        defines how many iterations the table joiner is allowed to do
        :param parts:
        defines which parquet of the dresden web tables is loaded. The integers in this list should be between [0,499].
        If not given, DataXFormer will assume parts = [0]
        :param db_path:
        takes a path to a duckDB database file, if user wants to use another database than dresden web tables or the
        user wants to exploit disk for the database. If given parameter parts is ignored
        :param use_table_joiner:
        decides if DataXFormer should use the TableJoiner Component
        :param verbose:
        decides if DataXFormer should print useful information
        :param debug:
        decides if the sql-query is limited to 50 to cut runtime for testing.
        """
        self.use_table_joiner = use_table_joiner
        self.verbose = verbose
        self.debug = debug

        if parts is None:
            parts = [0]
        self.parts = parts

        self.dbHandler = DBHandler(verbose=verbose, debug=debug, parts=parts, db_path=db_path)
        self.table_filter = TableFilter()

        self.delta_epsilon = delta_epsilon
        self.alpha = alpha
        self.tau = tau
        self.max_path = max_path

        self.__inp = None

        self.__lineage_answers = {}
        self.__lineage_tables = {}
        self.__answer_scores = {}
        self.__table_scores = {}

        if not (1 > alpha > 0):
            print("WARNING: smoothing_factor alpha should always be only slightly lower than one")

    def expectation_maximization(self, examples: pd.DataFrame, inp: pd.DataFrame):
        """
        This functions implements the Expectation-Maximization algorithm as describe in the paper for DataxFormer

        Parameters:
        ------------
        :param examples: A pandas DataFrame that contains all given Examples for the transformation
        :param inp: A pandas DataFrame that contains all query values
        :return: A pandas Dataframe that contains all found transformation with an extra row that contains the score
        of the transformation
        """


        # setting up environment of loop
        self.__inp = inp.copy()

        answers = examples.copy()
        tables = None
        finishedQuerying = False
        delta_score = 0

        for index, row in examples.iterrows():
            self.__answer_scores[tuple(row)] = 1.0

        iteration = 0

        while not finishedQuerying or delta_score > self.delta_epsilon:

            if self.verbose:
                print(f"######## Iteration {iteration} ########", '\n')
                iteration += 1

            new_value = False
            if not finishedQuerying:

                # get direct Transformation candidate
                tables = self.dbHandler.fetch_candidates(answers, tau=self.tau)

                # get indirect Transformation candidates
                if self.use_table_joiner:
                    # TODO call table joiner component
                    joined_tables = []



                # [TABLE, colx1, ...., coly]
                # TODO union tables
                if self.verbose:
                    print('\n', "############### Table Candidates ##############")
                    print(tables, '\n')

                for index, table in tables.iterrows():
                    table_id = table[0]

                    rows = self.dbHandler.fetch_table_columns(table)

                    # filter out tables where col-row alignment does not match or functional dependency does not hold
                    if not self.table_filter.filter(examples, rows, self.tau):
                        if self.verbose:
                            print(f"Filtered table {table_id}")
                        continue

                    input_columns = list(inp.columns)
                    rows_columns = list(rows.columns)

                    if self.verbose:
                        print(f"For Table {table_id}")
                        print("Checking Rows:")
                        print(rows)
                        print('\n')

                    # setting up a sql-query to find all transformations
                    select_statement = ""
                    for col_row in rows_columns:
                        select_statement += f" \"{col_row}\","
                    select_statement = select_statement[:len(select_statement) - 1]

                    on_statement = ""
                    for col_row, col_in in zip(rows_columns[:len(rows_columns) - 1],
                                               input_columns[:len(input_columns) - 1]):
                        on_statement += f"\"{col_row}\"=\"{col_in}\" AND "
                    on_statement = on_statement[: len(on_statement) - 5]

                    transformation_query = f"SELECT {select_statement} " \
                                           f"FROM rows JOIN inp " \
                                           f"ON ({on_statement})"

                    # find all transformations in table
                    possible_candidates = duckdb.query(transformation_query).to_df()

                    if possible_candidates.empty:
                        continue

                    if self.verbose:
                        print(f"Found possible answers in Table {table_id}:")
                        print(possible_candidates)
                        print('\n')

                    # updating the lineage of table
                    if not possible_candidates.empty:
                        possible_candidates.columns = answers.columns
                        answers = pd.concat([answers, possible_candidates], axis=0, ignore_index=True)
                        answers.drop_duplicates(inplace=True)

                        self.__lineage_tables[tuple(table)] = possible_candidates
                        self.__lineage_tables[tuple(table)].drop_duplicates(inplace=True)

                    # updating lineage of answers
                    for index, candidate in possible_candidates.iterrows():

                        key = tuple(candidate)
                        if self.__lineage_answers.get(key, pd.DataFrame()).empty:
                            new_value = True
                        self.__lineage_answers[key] = pd.concat(
                            (self.__lineage_answers.get(key, pd.DataFrame(columns=tables.columns)),
                             pd.DataFrame([table])),
                            axis=0)

            if not new_value:
                if self.verbose:
                    print("######## Finished finding new Answers ########")
                finishedQuerying = True

            if self.verbose:
                print(f"Possible Answers:\n {answers}\n")

            # Update Table Score
            self.__updateTableScore(answers, tables)

            # Update Answer Score
            old_a = self.__answer_scores.copy()
            self.__updateAnswerScores(answers)

            # calculate delta and compare to delta_epsilon
            delta_score = 0
            for key in self.__answer_scores:
                if key in old_a:
                    delta_score += abs(self.__answer_scores[key] - old_a[key])
                else:
                    delta_score += abs(self.__answer_scores[key])

        if True:
            print(self.__table_scores)
            print(self.__answer_scores)

        # add answers scores to answers dataframe
        # TODO use unique answer score column name
        score_values = pd.Series(np.fromiter((
            self.__answer_scores.get(tuple(row), 1.0) for index, row in answers.iterrows()),
            dtype=np.float),
            name="dataxformer_answer_score",
            dtype=np.float
        )

        return pd.concat((answers, score_values), axis=1)

    def __updateTableScore(self, answers, tables: pd.DataFrame):
        """
        Implements the UpdateTableScore step of the Expectation-Maximization algoritm

        :param answers: All found transformations until now
        :param tables: All tables that were found to contain at least tau transformations
        :return:
        """

        for index, table in tables.iterrows():
            good = 0
            bad = 0

            coveredX = self.__lineage_tables.get(tuple(table),
                                                 pd.DataFrame(columns=answers.columns))

            if coveredX.empty:
                continue

            # Incrementation step for good and bad counter
            for index, possible_transformation in coveredX.iterrows():
                if self.__isMax(tuple(possible_transformation), answers):
                    good += self.__answer_scores.get(tuple(possible_transformation), 1.0)
                else:
                    bad += 1

            # calculation of unseenX
            unseenX = self.__getUnseenX(coveredX, answers)

            score_values = pd.Series(np.fromiter((
                self.__answer_scores.get(tuple(row), 1.0) for index, row in unseenX.iterrows()),
                dtype=np.float))

            unseenX = pd.concat((unseenX, score_values), axis=1)

            # preparing group by to fit all x_columns
            groupby_clause = ""

            for x_column in unseenX.columns[:len(unseenX.columns) - 1]:
                groupby_clause += f"{x_column}, "

            groupby_clause = groupby_clause[:len(groupby_clause) - 2]

            # queries to calculate the sum of each maximum score for x_values
            sub_query = f"SELECT MAX(\"{unseenX.columns[-1]}\") as \"max_scores\" " \
                        f"FROM unseenX " \
                        f"GROUP BY {groupby_clause}"

            query = f"SELECT SUM(\"max_scores\") " \
                    f"FROM ({sub_query})"

            sum_value = duckdb.query(query).to_df().iat[0, 0]

            # Insert here query for belief in table if such a table exists. Else use 0.5 as standard for every table
            prior = 0.5

            # formular for table_score
            score = self.alpha * ((prior * good) / (prior * good + (1 - prior) * (bad + sum_value)))

            self.__table_scores[tuple(table)] = score

    def __isMax(self, candidate: tuple, answers: pd.DataFrame) -> bool:
        """
        Checks if given candidate has the highest score of all transformations for the same x value

        Parameters:
        ------------
        :param candidate: the transformation that should be checked
        :param answers: all found transformations until now
        :return: a boolean that is true if score is the highest
        """

        # preparing where clause for all x_columns
        where_clause = ""
        for column, x_value in zip(answers.columns[:len(answers.columns) - 1], candidate):
            where_clause += f"answers.\"{column}\"='{x_value}' AND "
        where_clause = where_clause[:len(where_clause) - 5]

        # query answers to find all other possible transformations that have the same x values
        query = f"SELECT * " \
                f"FROM answers " \
                f"WHERE {where_clause}"

        similar_transformations = duckdb.query(query).to_df()

        # check if subjects score is greater than its brethren
        all_scores = np.fromiter(
            (self.__answer_scores.get(
                tuple(row), 1.0) for index, row in similar_transformations.iterrows()), np.float)

        return not (all_scores > self.__answer_scores.get(candidate, 1.0)).any()

    def __getUnseenX(self, coveredX: pd.DataFrame, answers: pd.DataFrame):
        """
        Implements the retrieval of the complement of coveredX

        Parameters:
        -------------
        :param coveredX: All transformations that are covered by the table
        :param answers: all transformations found until now
        :return: A pandas DataFrame that is answers - coveredX
        """

        # generate where clause that can handle all x_columns
        where_clause = ""
        for column, x_value in zip(answers.columns[:len(answers.columns) - 1],
                                   coveredX.columns[:len(coveredX.columns) - 1]):
            where_clause += f"answers.\"{column}\" <> coveredX.\"{x_value}\" OR "
        where_clause = where_clause[:len(where_clause) - 4]

        # query database to get complement
        query = f"SELECT DISTINCT answers.* " \
                f"FROM answers, coveredX " \
                f"WHERE {where_clause}"
        complement = duckdb.query(query).to_df()
        return complement

    def __updateAnswerScores(
            self,
            answers: pd.DataFrame
    ):
        for x in self.__inp.iloc[:, :-1].to_numpy():
            answers_x = self.__get_answers(answers, x)

            # extract all distinct tuples (tableId, columnId1, ...) for x's answers
            all_tables = [tuple(table) for _, a in answers_x.iterrows()
                          for _, table in self.__lineage_answers[tuple(a)].iterrows()]
            all_tables = set(all_tables)  # remove duplicates

            score_of_none = 1.
            for table in all_tables:
                score_of_none *= (1 - self.__table_scores[table])
                for _, a in answers_x.iterrows():
                    a = tuple(a)
                    self.__answer_scores[a] = 1.

                    if table in self.__lineage_answers[tuple(a)]["TableId"]:
                        self.__answer_scores[a] += self.__table_scores[table]
                    else:
                        self.__answer_scores[a] += (1 - self.__table_scores[table])

            answer_score_sum = sum(self.__answer_scores.values())
            for _, a in answers_x.iterrows():
                self.__answer_scores[tuple(a)] /= score_of_none + answer_score_sum

    def __get_answers(self, answers: pd.DataFrame, x: np.ndarray):
        where_clause = ""
        for col, val in zip(answers.columns[:-1], x):
            where_clause += f"answers.\"{col}\" = '{val}' AND "
        where_clause = where_clause[:len(where_clause) - 4]

        query = f"SELECT DISTINCT answers.* " \
                f"FROM answers " \
                f"WHERE {where_clause}"

        return duckdb.query(query).to_df()
