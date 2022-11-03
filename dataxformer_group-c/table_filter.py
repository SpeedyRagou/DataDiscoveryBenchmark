import duckdb
import numpy as np
import pandas as pd

from db_handler import DBHandler


class TableFilter:
    """
    Filters table candidates.

    Parameters
    ----------
    db_handler : DBHandler
        Object for Database Interaction.
    """

    def __init__(self, db_handler: DBHandler):
        self.db_handler = db_handler

    def filter(self, examples: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
        """
        Takes table candidates, filters out results where functional dependencies do not hold or
        column-row alignments do not match, and returns filtered candidates.

        Parameters
        ----------
        examples : pd.DataFrame
            DataFrame of examples, e.g.

                 0    1    2
            0  x11  x12  y1
            1  x21  x22  y2

        candidates : pd.DataFrame
            DataFrame containing candidate table-IDs and the column-IDs, which are covered by the examples

        Returns
        -------
        pd.DataFrame
            Filtered candidate tables/columns.
        """
        # Build dict of tables: {int id_1: pd.DataFrame table_1, int id_2: pd.DataFrame table_2, ...}
        tables = {}
        table_ids = duckdb.query(f"SELECT DISTINCT TableId "
                                 f"FROM candidates").to_df()
        for i in table_ids.TableId:
            tables[i] = self.db_handler.fetch_table(i)

        # Check functional dependencies (FD)
        for t in tables:
            table = tables[t]
            # Collect all ColumnId-pairs of table
            col_ids = duckdb.query(f"SELECT DISTINCT ColumnId, ColumnId_2 "
                                   f"FROM candidates "
                                   f"WHERE TableId = {t}").to_df()
            print(col_ids)
            # Check if FD holds for these Column-pairs
            for col_id_1, col_id_2 in zip(col_ids.iloc[:, 0], col_ids.iloc[:, 1]):
                # Create DataFrame containing rows that violate the FD. Empty if FD holds.
                print(table.columns)
                fd_violations = duckdb.query(f"SELECT * "
                                             f"FROM table t1, table t2 "
                                             f"WHERE t1.`{table.columns[col_id_1]}` = t2.`{table.columns[col_id_1]}` "
                                             f"AND t1.`{table.columns[col_id_2]}` <> t2.`{table.columns[col_id_2]}` "
                                             ).to_df()
                # OPTION 2:
                # fd_violations = duckdb.query(f"SELECT * "
                #                              f"FROM table "
                #                              f"GROUP BY `{table.columns[col_id_1]}` "
                #                              f"HAVING COUNT (DISTINCT `{table.columns[col_id_2]}`) > 1").to_df()
                if not fd_violations.empty:     # if FD violated, delete entry in candidates
                    candidates = candidates.drop(candidates[(candidates['TableId'] == t) &
                                                            (candidates['ColumnId'] == col_id_1) &
                                                            (candidates['ColumnId_2'] == col_id_2)].index)

        # Check column-row alignment
        # TODO: Implement checking of column-row alignment!
        # for t in tables:
        #     for x, y in zip(examples.iloc[:, 0], examples.iloc[:, 1]):
        #         pass

        return candidates


if __name__ == "__main__":
    ex = None
    ca = pd.DataFrame({'TableId': [10367, 13174, 13174, 13174, 13174, 13174, 13174, 13174, 13174, 13174],
                       'ColumnId': [2, 4, 5, 21, 23, 7, 18, 19, 15, 6],
                       'ColumnId_2': [8, 2, 2, 2, 2, 2, 2, 2, 2, 2]})
    tf = TableFilter(DBHandler())
    print(tf.filter(ex, ca))
