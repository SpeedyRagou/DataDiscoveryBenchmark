import duckdb
import numpy as np

from datadiscoverybench.utils import load_dresden_db
from typing import List
import pandas as pd


class DBHandler:
    """
    Provides an interface for database interaction.

    Parameters
    ----------
    debug : bool
        If true, the number of result rows is limited to 10.
    """
    def __init__(self, debug: bool = True):
        self.con = duckdb.connect(database=':memory:')
        self.debug = debug

        load_dresden_db(self.con)

    def __generate_query(self, examples: np.ndarray, tau: int = 2) -> str:
        x_cols = len(examples) - 1

        # outer select
        query = f"SELECT colX1.TableId, "
        for x in range(0, x_cols):
            query += f"colX{x + 1}.ColumnId, "
        query += f"colY.ColumnId " \
                 f"FROM "

        # subquery for x columns and y column
        for x in range(0, x_cols):
            joint_list = "','".join(examples[x])

            query += f"\n   (SELECT TableId, ColumnId " \
                     f"\n   FROM AllTables " \
                     f"\n   WHERE CellValue IN ('{joint_list}') " \
                     f"\n   GROUP BY TableId, ColumnId " \
                     f"\n   HAVING COUNT(DISTINCT CellValue) >= {tau}) AS colX{x + 1},\n"

        joint_list = "','".join(examples[x_cols])
        query += f"\n   (SELECT TableId, ColumnId " \
                 f"\n   FROM AllTables " \
                 f"\n   WHERE CellValue IN ('{joint_list}') " \
                 f"\n   GROUP BY TableId, ColumnId " \
                 f"\n   HAVING COUNT(DISTINCT CellValue) >= {tau}) AS colY "

        query += f"\nWHERE colX1.TableId = colY.TableId "

        for x in range(0, x_cols - 1):
            query += f"\nAND colX{x + 1}.TableId = colX{x + 2}.TableId "
            query += f"\nAND colX{x + 1}.ColumnId <> colX{x + 2}.ColumnId "

        for x in range(0, x_cols):
            query += f"\nAND colX{x + 1}.ColumnId <> colY.ColumnId "

        if self.debug:
            query += "\nLIMIT 10;"

        print(query)
        return query

    def fetch_candidates(self, examples: pd.DataFrame, tau: int = 2) -> pd.DataFrame:
        """
        Fetches all candidate column combinations that satisfy at least tau examples per column.

        Parameters
        ----------
        examples : pd.DataFrame
            DataFrame of examples, e.g.

                 0    1    2
            0  x11  x21  y1
            1  x21  x22  y2

        tau : int
            Minimum number of examples per column.

        Returns
        -------
        pd.DataFrame
            Candidate tables/columns.
        """
        print(examples.to_numpy().T)
        return self.con.execute(self.__generate_query(examples.to_numpy().T, tau=tau)).fetch_df()

    def fetch_table(self, table_id: int) -> pd.DataFrame:
        """
        Fetches table with given ID to a pandas DataFrame-

        Parameters
        ----------
        table_id : int
            Table ID.

        Returns
        -------
        pd.DataFrame
            Table as Pandas DataFrame.
        """
        table_content = self.con.execute(f"SELECT CellValue, ColumnId "
                                         f"FROM AllTables "
                                         f"WHERE TableId = {table_id} "
                                         f"ORDER BY ColumnId, RowId ").fetch_df()

        table = pd.DataFrame()
        for col_id, column_content in table_content.groupby(['ColumnId']):
            table[col_id] = list(column_content['CellValue'])

        return table


if __name__ == "__main__":
    db = DBHandler()
    ex = pd.DataFrame([["x11", "x12", "y1 "], ["x21", "x22", "y2 "]])
    db.fetch_candidates(ex)
    #db.fetch_table(1)


