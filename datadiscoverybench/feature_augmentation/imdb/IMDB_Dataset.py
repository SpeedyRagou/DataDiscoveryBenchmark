from datadiscoverybench.feature_augmentation.Data import Dataset
import os
import urllib.request
import pandas as pd
import duckdb
from datadiscoverybench.utils import dir_path

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

class IMDB(Dataset):

    def __init__(self, number_rows=None):

        if not os.path.isdir(dir_path + '/data'):
            os.mkdir(dir_path + '/data')

        if not os.path.isdir(dir_path + '/data/imdb'):
            os.mkdir(dir_path + '/data/imdb')

        if not os.path.isfile(dir_path + '/data/imdb/' + 'title.ratings.tsv.gz'):
            urllib.request.urlretrieve("https://datasets.imdbws.com/title.ratings.tsv.gz",
                                       dir_path + '/data/imdb/' + 'title.ratings.tsv.gz')
        ratings = pd.read_csv(dir_path + '/data/imdb/' + 'title.ratings.tsv.gz', compression='gzip', header=0,
                              delimiter='\t')

        if not os.path.isfile(dir_path + '/data/imdb/' + 'title.basics.tsv.gz'):
            urllib.request.urlretrieve("https://datasets.imdbws.com/title.basics.tsv.gz",
                                       dir_path + '/data/imdb/' + 'title.basics.tsv.gz')
        titles = pd.read_csv(dir_path + '/data/imdb/' + 'title.basics.tsv.gz', compression='gzip', header=0,
                             delimiter='\t')

        if type(number_rows) != type(None):
            titles = titles.head(number_rows)


        con = duckdb.connect(database=':memory:')
        self.data = con.execute('''
                              SELECT * 
                              FROM ratings 
                              LEFT JOIN titles 
                              ON titles.tconst=ratings.tconst 
                              ORDER BY titles.tconst ASC;
                           ''').fetch_df()
        self.data.drop(['tconst_2'], axis=1, inplace=True, errors='ignore')

        self.id_column_name = 'tconst'

    def get_df(self):
        return self.data

