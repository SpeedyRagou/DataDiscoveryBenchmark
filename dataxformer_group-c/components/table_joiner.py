from typing import List, Tuple, Any, Dict
import pandas as pd
import duckdb
from .db_handler import DBHandler

from .table_filter import TableFilter


class TableJoiner:
    """
    Joining Tables to find indirect transformations.

    Parameters
    ----------
    max_length : int
        Maximum length of path when searching for indirect transformations.

    tau : int
        Minimum number of examples per column.

    debug : bool
        If true, the number of result rows is limited.
    """

    def __init__(self, db_handler: DBHandler, max_length: int, tau: int, verbose: bool = False, debug: bool = False):
        self.__db_handler = db_handler
        self.max_length = max_length
        self.tau = tau
        self.verbose = verbose
        self.debug = debug
        self.__seen_tables: pd.DataFrame
        self.__cur_table_id = -1
        self.table_dict = {}

    def execute(self, examples: pd.DataFrame, query_values: pd.DataFrame, t_e: pd.DataFrame) -> Tuple[List[Any], Dict[Any, Any]]:
        """
        Joining Tables to find indirect transformations.

        Parameters
        ----------
        examples : pd.DataFrame
            DataFrame of examples, e.g.
                 0    1    2
            0  x11  x12  y1
            1  x21  x22  y2

        t_e : pd.DataFrame
            Initial tables T_E stored as indices.

        Returns
        -------
        List[pd.DataFrame]
            Returns tables.
        """

        if self.verbose:
            print("############# Starting TableJoiner ###############\n")


        tables = []
        current_table_paths = []
        joined_tables = {}

        self.__seen_tables = t_e.copy()

        path = 1

        while abs(path) < self.max_length or False:  # TODO: Implement second condition!
            if self.verbose:
                print(f"############# Starting TableJoiner (Path={path}) ###############\n")

            if path == 1:
                new_tables, new_table_paths = self.__table_join_iteration(
                    path,
                    query_values.iloc[:, :-1],
                    examples.iloc[:, -1],
                    query_values.iloc[:len(examples.columns) - 1],
                )
            else:
                new_table_paths = []
                new_tables = []

                for cur_path, joinable_table in current_table_paths:
                    t_z, z_i = joinable_table

                    new_tables, tmp_table_paths = self.__table_join_iteration(
                        path,
                        t_z,
                        examples.iloc[:, -1],
                        query_values.iloc[:len(examples.columns)-1],
                        joinable_table=joinable_table
                    )
                    new_table_paths += tmp_table_paths

            tables += new_tables
            current_table_paths = new_table_paths
            path += 1

        return tables, self.table_dict

    def __table_join_iteration(
            self,
            path: int,
            x_values: pd.DataFrame,
            y_values: pd.Series,
            query_values: pd.DataFrame,
            joinable_table: Tuple[pd.Series, int] = None
    ):
        # Find all tables that contain the x values of the examples:
        t_x = self.__db_handler.query_for_tables(x_values, self.tau)

        # Exclude tables that provide direct transformations of the examples:
        t_x = t_x.merge(self.__seen_tables, how='left', indicator=True).loc[
            lambda x: x['_merge'] == 'left_only'].iloc[:, :-2]

        self.__seen_tables = pd.concat([self.__seen_tables, t_x], axis=0, ignore_index=True)

        tables = []
        current_table_paths = []

        if self.verbose:
            print(f"Found:\n{t_x}")

        for _, t_z in t_x.iterrows():
            # t_z = [table_id, col1_id, col2_id]
            # fetch tables that contain x values for path = 1 or z values for path > 1
            table_z = self.__db_handler.fetch_table(t_z[0])
            x_ids = t_z[1:]


            # for path > 1 join table of previous iteration on new table found for z
            if joinable_table is not None:
                table_p, z = joinable_table

                table_z = pd.merge(
                    table_z,
                    table_p,
                    left_on=table_z.columns[t_z[1]],
                    right_on=table_p.columns[z]
                )

            # Extract columns for Join by checking FD:
            columns = self.find_join_columns(table_z, list(t_z[1:]))

            if self.verbose:
                print(f"Possible Joinable Columns:\n{columns}")

            for z_i in columns:
                # fetch tables that can be joined on x (path = 1) or z (path > 1) and contain y
                # next part requires only tables that contain y,
                # we are not interested in other tables
                try:
                    joinable_tables = self.__db_handler.fetch_candidates(
                        pd.DataFrame({'x': table_z[z_i], 'y': y_values}),
                        tau=self.tau
                    )
                    if self.verbose:
                        print(f"\nFound possible Tables to Join:\n{joinable_tables}\n")

                except:
                    if self.verbose:
                        print("Catched an error with z-values:")
                        print(table_z[z_i])
                        continue

                if len(joinable_tables) > 0:
                    for _, t_j in joinable_tables.iterrows():
                        # join current table with fetched join candidate table containing y
                        table_j = self.__db_handler.fetch_table(t_j[0])
                        z_j_id, z_y_id = t_j[1], t_j[2]
        
                        query = f"SELECT * " \
                                f"FROM table_z JOIN table_j " \
                                f"ON (table_z.\"{table_z.columns[z_i]}\" = table_j.\"{table_j.columns[z_j_id]}\")"
                        try:
                            joined_table = duckdb.query(query).to_df()
                        except:
                            if self.verbose:
                                print("Catched an error:\n")
                                print(table_j)
                                print(table_z)
                                continue

                        on_clause = ""
                        for ex_col, x_id in zip(x_values.columns, x_ids):
                            on_clause += f"x_values.\"{ex_col}\" = joined_table.\"{x_id}\" AND "
                        on_clause = on_clause[:-4]

                        # check if joined table contains at least tau examples
                        # maybe we do exact row alignment checks later
                        # TODO use examples here instead of x_values (maybe???)
                        query = f"SELECT joined_table.* " \
                                f"FROM joined_table JOIN x_values " \
                                f"ON ({on_clause});"

                        joined_examples_table = duckdb.query(query).to_df()

                        if len(joined_examples_table) > self.tau:

                            # retrieve joined table with only x and y values
                            x_ids_selection = ""
                            for x_id in x_ids:
                                x_ids_selection += f"table_z.\"{x_id}\", "

                            query = f"SELECT {x_ids_selection} table_j.\"{table_j.columns[z_y_id]}\" " \
                                    f"FROM table_z JOIN table_j " \
                                    f"ON (table_z.\"{table_z.columns[z_i]}\" = table_j.\"{table_j.columns[z_j_id]}\")"

                            important_columns_table = duckdb.query(query).to_df()

                            on_clause = ""
                            for ex_col, x_id in zip(x_values.columns, important_columns_table.columns[:len(important_columns_table) - 1]):
                                on_clause += f"query_values.\"{ex_col}\" = important_columns_table.\"{x_id}\" AND "
                            on_clause = on_clause[:-4]

                            # filter table for only rows with interesting x_values to reduce ram load
                            query = f"SELECT important_columns_table.* " \
                                    f"FROM important_columns_table JOIN query_values " \
                                    f"ON ({on_clause});"

                            row_filtered_table = duckdb.query(query).to_df()

                            # build unique identification for new joined table and store table for retrieval
                            series = [self.__cur_table_id] + [i for i in range(len(row_filtered_table.columns))]

                            if self.verbose:
                                print()
                                print(f"Added:\n{series}\n for \n{row_filtered_table}")

                            series = tuple(series)
                            tables += [series]
                            self.table_dict[series] = row_filtered_table

                            self.__cur_table_id -= 1

                    current_table_paths += [(path, (table_z, z_i))]

        return tables, current_table_paths

    def find_join_columns(self, table: pd.DataFrame, x_col_ids: List[int]) -> List[int]:
            """
            Extracts columns for Join by checking FD.

            Parameters
            ----------
            table : pd.DataFrame
                Table from set of tables that contain the x values of the examples.

            examples_x : pd.DataFrame
                DataFrame of examples (only x-values).

            x_col_ids : List[int]
                .

            Returns
            -------
            List[int]
                Returns list of columns ids for Join.
            """
            columns = []
            for z in range(len(table.columns)):
                if z in x_col_ids:
                    continue
                sub_table = table.iloc[:, x_col_ids + [z]]
                if TableFilter.check_fd(sub_table):
                    columns += [z]

            return columns


if __name__ == "__main__":
    df1 = pd.DataFrame({'City': ['New York', 'Chicago', 'Tokyo', 'Paris', 'New Delhi'],
                        'Temp': [59, 29, 73, 56, 48]})

    df2 = pd.DataFrame({'City': ['London', 'New York', 'Tokyo', 'New Delhi', 'Paris'],
                        'Temp': [55, 55, 73, 85, 56]})

    df = df1.merge(df2, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']

    print(df.iloc[:, :-1])

    # City, Country -> Zip
    # City, Country -|-> Temp
    t1 = pd.DataFrame({'City':    ["A",  "A",  "C",  "D"],    # X1
                       'Country': ["AT", "AT", "CT", "DT"],   # Z
                       'Zip':     [1,    1,    3,    4],      # Y
                       'Temp':    [55,   55,   73,   85]})    #

    # print(find_join_columns(t1, [0, 1]))

