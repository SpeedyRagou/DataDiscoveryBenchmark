from typing import List
import pandas as pd
import duckdb

from table_filter import TableFilter


class TableFilter:
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

    def __init__(self, max_length: int, tau: int, debug: bool = False):
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
                t_x = self.query_for_tables(examples.iloc[:, :-1], self.tau)
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

    def query_for_tables(self, examples_x: pd.DataFrame, tau: int) -> pd.DataFrame:
        """
        Queries for tables that contain x-values of the examples.

        Parameters
        ----------
        examples_x : pd.DataFrame
            DataFrame of examples (only x-values).

        tau : int
            Minimum number of examples per column.

        Returns
        -------
        pd.DataFrame
            Returns table, storing table-column indices of tables, that contain x-values of the examples.
        """
        examples_x = examples_x.to_numpy().T
        x_cols = len(examples_x)

        # outer select
        query = f"SELECT colX1.TableId, "
        for x in range(0, x_cols):
            query += f"colX{x + 1}.ColumnId, "
        query += f"colY.ColumnId " \
                 f"FROM "

        # subquery for x columns
        for x in range(0, x_cols):
            joint_list = "','".join(set(examples_x[x]))

            query += f"\n   (SELECT TableId, ColumnId " \
                     f"\n   FROM AllTables " \
                     f"\n   WHERE CellValue IN ('{joint_list}') " \
                     f"\n   GROUP BY TableId, ColumnId " \
                     f"\n   HAVING COUNT(DISTINCT CellValue) >= {tau}) AS colX{x + 1},\n"

        for x in range(0, x_cols - 1):
            query += f"\nAND colX{x + 1}.TableId = colX{x + 2}.TableId "
            query += f"\nAND colX{x + 1}.ColumnId <> colX{x + 2}.ColumnId "

        if self.debug:
            query += "\nLIMIT 50;"

        con = duckdb.connect(database=':memory:')

        return con.execute(query).fetch_df()

    def find_join_columns(self, table: pd.DataFrame, examples_x: pd.DataFrame) -> List[pd.DataFrame]:
        """
        Extracts columns for Join by checking FD.

        Parameters
        ----------
        table : pd.DataFrame
            Table from set of tables that contain the x values of the examples.

        examples_x : pd.DataFrame
            DataFrame of examples (only x-values).

        Returns
        -------
        List[pd.DataFrame]
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