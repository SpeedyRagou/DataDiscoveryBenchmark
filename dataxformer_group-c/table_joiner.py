from typing import List
import pandas as pd
import duckdb
from db_handler import DBHandler

from table_filter import TableFilter


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
        path = 1

        while abs(path) > self.max_length or False:  # TODO: Implement second condition!

            if path == 1:

                # Find all tables that contain the x values of the examples:
                t_x = self.__db_handler.query_for_tables(examples.iloc[:, :-1], self.tau)
                # Exclude tables that provide direct transformations of the examples:
                t_x = t_x.merge(t_e, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
                t_x = t_x.iloc[:, :-1]  # Delete '_merge'-column

                for _, t in t_x.iterrows():
                    # Extract columns for Join by checking FD:
                    # t = ...add descriptive comment
                    table = self.__db_handler.fetch_table(t[0])
                    columns = self.find_join_columns(table, examples.iloc[:, :-1], t[1:])
                    for z_i in columns:
                        pass  # TODO: Finish implementation!

                        t_j = self.find_joinable_tables(z_i, examples, t)
                        # if len(t_j) > 0 and covered_examples(t_j, examples) > self.tau:

            else:
                pass  # TODO: Finish implementation!

        return NotImplemented

    def find_join_columns(self, table: pd.DataFrame, examples_x: pd.DataFrame, x_col_ids: List[int]) -> List[pd.DataFrame]:
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
        List[pd.DataFrame]
            Returns list of columns for Join.
        """
        # TODO: Use TableFilter.check_fd() here

        return NotImplemented

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