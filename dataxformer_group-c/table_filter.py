import duckdb
import pandas as pd
import numpy as np


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

        # TODO: Before FD-check do some cleaning on table to remove NaN values
        # here

        # query to extract all rows that violate functional dependency
        query = f"SELECT * " \
                f"FROM t t1 , t t2 " \
                f"WHERE t1.\"{table.columns[-1]}\" <> t2.\"{table.columns[-1]}\" "
        for i in range(len(t.columns)-1):
            query += f"AND t1.\"{table.columns[i]}\" = t2.\"{table.columns[i]}\" " \

        fd_violations = duckdb.query(query).to_df()

        if not fd_violations.empty:  # fd_violations contains all rows that violate the functional dependency
            return False
        return True

    def filter(self, examples: pd.DataFrame, table: pd.DataFrame, tau: int) -> bool:
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

        tau : int
            Minimum number of examples per column.

        Returns
        -------
        bool
            Returns whether column-row alignment is correct and functional dependency holds.
        """
        if len(examples.columns) != len(table.columns):
            raise ValueError("Table does not have the same amount of columns as examples.")

        table_passes = False

        x = examples.iloc[:, :-1]   # x = all columns except last
        y = examples.iloc[:, -1]    # y = last column

        count = 0
        # Check column-row alignment of examples in table
        for (_, row_X), row_Y in zip(x.iterrows(), y):
            # Get all indices of rows that contain row_X = [x1, x2, ..., xn] in the x-columns:
            x_idx = table[(table.iloc[:, :-1] == row_X.to_numpy()).all(1)].index.tolist()
            # Get all indices of rows that contain y in the last column (y-column):
            y_idx = table.index[table.iloc[:, -1] == row_Y].tolist()
            if x_idx:
                if not set(x_idx).isdisjoint(y_idx):    # same as x_idx INTERSECTS y_idx (most efficient way)
                    count += 1
                if count >= tau:                        # tau examples fulfill column-row alignment
                    table_passes = True
                    break

        # Check functional dependency
        #table_passes = self.check_fd(table)

        return table_passes


if __name__ == "__main__":
    # ca = pd.DataFrame({'TableId': [10367, 13174, 13174, 13174, 13174, 13174, 13174, 13174, 13174, 13174],
    #                    'ColumnId': [2, 4, 5, 21, 23, 7, 18, 19, 15, 6],
    #                    'ColumnId_2': [8, 2, 2, 2, 2, 2, 2, 2, 2, 2]})

    '''Tables for testing'''
    table1 = pd.DataFrame({'xCol': ['FCB', 'BVB', 'HSV', 'ACM', 'RM'],
                           'yCol': ['Munich', 'Dortmund', 'Hamburg', 'Milan', 'Madrid']})
    table2 = pd.DataFrame({'xCol': ['FCB', 'BVB', 'HSV', 'ACM', 'RM', 'FCB'],
                           'yCol': ['Munich', 'Dortmund', 'Hamburg', 'Milan', 'Madrid', 'Barcelona']})
    table3 = pd.DataFrame({'xCol': ['FCB', 'BVB', 'HSV', 'ACM', 'RM'],
                           'yCol': ['Munich', 'Hamburg', 'Dortmund', 'Milan', 'Madrid']})
    table4 = pd.DataFrame({'xCol': ['FCB', 'BVB', 'HSV', 'ACM', 'RM', 'FCB'],
                           'yCol': ['Munich', 'Hamburg', 'Dortmund', 'Milan', 'Madrid', 'Barcelona']})
    table5 = pd.DataFrame({'xCol': ['FCB', 'BVB', 'HSV', 'ACM', 'RM', 'FCB'],
                           'xCo2': ['Ger', 'Ger', 'Ger', 'Ita', 'Esp', 'Esp'],
                           'yCol': ['Munich', 'Dortmund', 'Hamburg', 'Milan', 'Madrid', 'Barcelona']})

    '''Examples for testing'''
    ex1 = pd.DataFrame({'xCol': ['FCB', 'HSV'],
                        'yCol': ['Munich', 'Hamburg']})
    ex2 = pd.DataFrame({'xCol': ['FCB', 'HSV'],
                        'xCo2': ['Ger', 'Ger'],
                        'yCol': ['Munich', 'Hamburg']})

    '''Execution of test'''
    tf = TableFilter()
    print(tf.filter(ex1, table1, 2))

    '''Get index of DataFrame for rows that are identical to elements of an array (Version B)'''
    # temp = pd.DataFrame({"A": [1, 2, 3, 4], "B": [4, 5, 6, 7], "C": [7, 8, 9, 10]})
    # x = np.array([[1, 4, 7], [4, 7, 10]])
    # array = temp.to_numpy()[:, None]
    # mask = (array == x).all(axis=-1).any(axis=-1)
    # print(temp.index[mask].tolist())
    # print([9, 8])

    '''Get index of DataFrame for rows that are identical to elements of an array (Version A)'''
    # df = pd.DataFrame([[0, 1], [2, 3], [4, 5]], columns=['A', 'B'])
    # a = np.array([0, 1])
    # index_list = df[(df == a).all(1)].index.tolist()
    # print(index_list)
