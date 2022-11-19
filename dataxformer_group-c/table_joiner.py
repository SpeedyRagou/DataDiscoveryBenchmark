from typing import List
import pandas as pd

from db_handler import DBHandler
from table_filter import TableFilter


class TableFilter:
    """
    Joining Tables to find indirect transformations.

    Parameters
    ----------
    max_length : int
        Maximum length of path when searching for indirect transformations.
    """

    def __init__(self, max_length: int):
        self.max_length = max_length
        self.dbHandler = DBHandler(verbose=False, debug=True,
                                   db_path="/home/becktepe/gittables_DXF_all.duckdb")

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
                t_x = self.dbHandler.fetch_candidates(examples.iloc[:, :-1], tau=self.tau)  # TODO: Is dbHandler.fetch_candidates() able to process examples without Y?
                # Exclude tables that provide direct transformations of the examples:
                t_x = t_x.merge(t_e, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
                t_x = t_x.iloc[:, :-1]

                for t in t_x:
                    # Extract columns for Join by checking FD:
                    columns = self.find_join_columns(t, examples.iloc[:, :-1])
                    for z_i in columns:
                        pass  # TODO: Finish implementation!

            else:
                pass  # TODO: Finish implementation!

        return NotImplemented

    def find_join_columns(self, table: pd.DataFrame, examples_x: pd.DataFrame) -> List[pd.DataFrame]:
        """
        Extracts columns for Join by checking FD.

        Parameters
        ----------
        table : pd.DataFrame
            Table from set of tables that contain the x values of the examples.

        examples_x : pd.DataFrame
            DataFrame of examples.

        Returns
        -------
        pd.DataFrame
            Returns list of columns for Join.
        """
        # TODO: Use TableFilter.check_fd() here

        return NotImplemented


if __name__ == "__main__":
    df1 = pd.DataFrame({'City': ['New York', 'Chicago', 'Tokyo', 'Paris', 'New Delhi'],
                        'Temp': [59, 29, 73, 56, 48]})

    df2 = pd.DataFrame({'City': ['London', 'New York', 'Tokyo', 'New Delhi', 'Paris'],
                        'Temp': [55, 55, 73, 85, 56]})

    df = df1.merge(df2, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']

    print(df.iloc[:, :-1])