import duckdb
from datadiscoverybench.utils import load_dresden_db
from typing import List
import pandas as pd


class DBHandler:
    """
    Provides an interface for database interaction.

    Parameters
    ----------
    debug : bool
        If true, the number of result rows is limited to 100.
    """
    def __init__(self, debug: bool = True):
        self.con = duckdb.connect(database=':memory:')
        self.debug = debug

        load_dresden_db(self.con)

    def __generate_query(self, examples: List[List[str]], tau: int = 2) -> str:
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

            query += f"(SELECT TableId, ColumnId " \
                     f"FROM AllTables " \
                     f"WHERE CellValue IN ('{joint_list}') " \
                     f"GROUP BY TableId, ColumnId " \
                     f"HAVING COUNT(DISTINCT CellValue) >= {tau}) AS colX{x + 1}, "

        joint_list = "','".join(examples[x_cols])
        query += f"(SELECT TableId, ColumnId " \
                 f"FROM AllTables " \
                 f"WHERE CellValue IN ('{joint_list}') " \
                 f"GROUP BY TableId, ColumnId " \
                 f"HAVING COUNT(DISTINCT CellValue) >= {tau}) AS colY "

        query += f"WHERE colX1.TableId = colY.TableId "

        for x in range(0, x_cols - 1):
            query += f"AND colX{x + 1}.TableId = colX{x + 2}.TableId "
            query += f"AND colX{x + 1}.ColumnId <> colX{x + 2}.ColumnId "

        for x in range(0, x_cols):
            query += f"AND colX{x + 1}.ColumnId <> colY.ColumnId "

        if self.debug:
            query += "LIMIT 100"

        return query

    def fetch_candidates(self, examples: List[List[str]], tau: int = 2) -> pd.DataFrame:
        """
        Fetches all candidate column combinations that satisfy at least tau examples per column.

        Parameters
        ----------
        examples : List[List[str]]
            List of examples, e.g.
            [[x11, x21, ...],
             [x21, x22, ...],
             ...
             [y1, y2, ...]]

        tau : int
            Minimum number of examples per column.

        Returns
        -------
        pd.DataFrame
            Candidate tables/columns.
        """
        return self.con.execute(self.__generate_query(examples, tau=tau)).fetch_df()

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
    db.fetch_table(1)


