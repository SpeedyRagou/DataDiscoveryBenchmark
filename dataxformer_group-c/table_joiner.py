from typing import List
import pandas as pd
import duckdb
from db_handler import DBHandler

from table_filter import TableFilter
from util import get_answers


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

    def __init__(self, db_handler: DBHandler, max_length: int, tau: int, debug: bool = False):
        self.__db_handler = db_handler
        self.max_length = max_length
        self.tau = tau
        self.debug = debug

    def execute(self, examples: pd.DataFrame, Q: set, t_e: pd.DataFrame) -> pd.DataFrame:
        """
        Joining Tables to find indirect transformations.

        Parameters
        ----------
        examples : pd.DataFrame
            DataFrame of examples, e.g.
                 0    1    2
            0  x11  x12  y1
            1  x21  x22  y2

        Q : set
            A set of input values Q

        t_e : pd.DataFrame
            Initial tables T_E stored as indices.

        Returns
        -------
        pd.DataFrame
            Returns tables.
        """
        tables = []
        current_table_paths = []

        path = 1

        while abs(path) > self.max_length or False:  # TODO: Implement second condition!

            if path == 1:

                # Find all tables that contain the x values of the examples:
                t_x = self.__db_handler.query_for_tables(examples.iloc[:, :-1], self.tau)
                # Exclude tables that provide direct transformations of the examples:
                t_x = t_x.merge(t_e, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
                t_x = t_x.iloc[:, :-1]  # Delete '_merge'-column
                x_ids = t_x.iloc[:, :-1]

                for _, t in t_x.iterrows():
                    # Extract columns for Join by checking FD:
                    # t = ...add descriptive comment
                    table = self.__db_handler.fetch_table(t[0])
                    columns = self.find_join_columns(table, t[1:])

                    for z_i in columns:
                        joinable_tables = self.__db_handler.fetch_candidates(
                            pd.DataFrame({'x': table[z_i], 'y': examples.iloc[:, -1]}),
                            tau=self.tau
                        )

                        if len(joinable_tables) > 0:
                            for _, t_j in joinable_tables.iterrows():
                                table_j = self.__db_handler.fetch_table(t_j[0])
                                z_j_id, z_y_id = t_j[1], t_j[2]

                                x_ids_selection = ""
                                for x_id in x_ids:
                                    x_ids_selection += f"table.\"{x_id}\", "

                                query = f"SELECT {x_ids_selection} table_j.\"{table.columns[z_j_id]}\" " \
                                        f"FROM table JOIN table_j " \
                                        f"ON (table.\"{table.columns[z_i]}\" = table_j.\"{table.columns[z_j_id]}\")"

                                joined_table = duckdb.query(query).to_df()

                                on_clause = ""
                                for ex_col, x_id in zip(range(examples.columns[:-1]), x_ids):
                                    on_clause += f"examples.\"{ex_col}\" = table.\"{x_id}\" AND "
                                on_clause = on_clause[:-4]

                                query = f"SELECT joined_table.* " \
                                        f"FROM joined_table JOIN examples " \
                                        f"ON ({on_clause});"

                                joined_examples_table = duckdb.query(query).to_df()

                                if len(joined_examples_table) > self.tau:
                                    tables += [joined_table]
                            current_table_paths += [(path, (t, z_i))]
            else:
                pass  # TODO: Finish implementation!

        return NotImplemented

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

    def find_joinable_tables(
            self,
            examples: pd.DataFrame,
            table: pd.DataFrame,
            col: str
    ) -> List[pd.DataFrame]:
        """
        ???

        Parameters
        ----------
        examples : pd.DataFrame


        table : pd.DataFrame

        col : str

        Returns
        -------
        List[pd.DataFrame]
            List of tables that can be joined on table[col]
        """
        raise NotImplemented


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

