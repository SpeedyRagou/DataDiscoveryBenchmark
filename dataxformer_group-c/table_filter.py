import duckdb
import pandas as pd


class TableFilter:
    """
    Filters table candidates.
    """

    def __init__(self):
        pass

    @staticmethod
    def check_fd(table: pd.DataFrame) -> bool:
        """
        Takes a candidate table and checks whether functional dependency (FD) holds.

        Parameters
        ----------
        table : pd.DataFrame
            A candidate table.

        Returns
        -------
        bool
            Returns whether functional dependency holds.
        """
        t = table
        # query to extract all rows that violate functional dependency
        query = f"SELECT * " \
                f"FROM t t1 , t t2 " \
                f"WHERE t1.\"{table.columns[0]}\" = t2.\"{table.columns[0]}\" " \
                f"AND t1.\"{table.columns[1]}\" <> t2.\"{table.columns[1]}\" "
        fd_violations = duckdb.query(query).to_df()

        if not fd_violations.empty:  # fd_violations contains all rows that violate the functional dependency
            return False
        return True

    def filter(self, examples: pd.DataFrame, table: pd.DataFrame) -> bool:
        """
        Takes a candidate table, checks whether column-row alignment of examples in that table match
        and whether functional dependency (FD) holds.

        Parameters
        ----------
        examples : pd.DataFrame
            DataFrame of examples, e.g.

                 0    1    2
            0  x11  x12  y1
            1  x21  x22  y2

        table : pd.DataFrame
            A candidate table.

        Returns
        -------
        bool
            Returns whether column-row alignment is correct and functional dependency holds.
        """
        table_passes = False

        # Check column-row alignment of examples in table
        for x, y in zip(examples.iloc[:, 0], examples.iloc[:, 1]):
            x_idx = table.index[table.iloc[:, 0] == x].tolist()
            y_idx = table.index[table.iloc[:, 1] == y].tolist()
            if x_idx:
                if set(x_idx).isdisjoint(y_idx):
                    return table_passes

        # Check functional dependency
        table_passes = self.check_fd(table)

        return table_passes


if __name__ == "__main__":
    # ca = pd.DataFrame({'TableId': [10367, 13174, 13174, 13174, 13174, 13174, 13174, 13174, 13174, 13174],
    #                    'ColumnId': [2, 4, 5, 21, 23, 7, 18, 19, 15, 6],
    #                    'ColumnId_2': [8, 2, 2, 2, 2, 2, 2, 2, 2, 2]})

    table1 = pd.DataFrame({'xCol': ['FCB', 'BVB', 'HSV', 'ACM', 'RM'],
                           'yCol': ['Munich', 'Dortmund', 'Hamburg', 'Milan', 'Madrid']})
    table2 = pd.DataFrame({'xCol': ['FCB', 'BVB', 'HSV', 'ACM', 'RM', 'FCB'],
                           'yCol': ['Munich', 'Dortmund', 'Hamburg', 'Milan', 'Madrid', 'Barcelona']})
    table3 = pd.DataFrame({'xCol': ['FCB', 'BVB', 'HSV', 'ACM', 'RM'],
                           'yCol': ['Munich', 'Hamburg', 'Dortmund', 'Milan', 'Madrid']})
    table4 = pd.DataFrame({'xCol': ['FCB', 'BVB', 'HSV', 'ACM', 'RM', 'FCB'],
                           'yCol': ['Munich', 'Hamburg', 'Dortmund', 'Milan', 'Madrid', 'Barcelona']})

    ex = pd.DataFrame({'xCol': ['FCB', 'HSV'],
                       'yCol': ['Munich', 'Hamburg']})

    tf = TableFilter()
    print(tf.filter(ex, table1))
