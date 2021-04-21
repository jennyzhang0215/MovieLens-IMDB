import os
import pandas as pd
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'movielens', 'raw')


class MovieLenData():
    def __init__(self, data_name):
        if data_name == 'ml-25m':
            MOVIE_FILE = 'movies.csv'
            LINK_FILE = 'links.csv'
            RATING_FILE = 'ratings.csv'
        else:
            raise NotImplementedError

        _movie_file = os.path.join(DATA_DIR, data_name, MOVIE_FILE)
        _link_file = os.path.join(DATA_DIR, data_name, LINK_FILE)

        if os.path.isfile(_movie_file):
            self.movies = self.read_movie(_movie_file)
        ## read the link file
        if os.path.isfile(_link_file):
            self.links = self.read_link(_link_file)

    def read_movie(self, file_name):
        df = pd.read_csv(file_name, header=0, sep=',', error_bad_lines=True, low_memory=False, dtype=str)
        year_l = []
        _year_token = ' (0000)'
        N_token = len(_year_token)
        wrong_num = 0
        for idx, row in df.iterrows():
            ## convert IMDB id from ####### to tt#######
            ## take the year from the title name
            origin_title = row['title'].strip('"').strip()
            title = origin_title[:-N_token]
            year = origin_title[-N_token+2:-1]

            try:
                assert int(year) > 1000
                df.at[idx, 'title'] = title
                year_l.append(year)
            except:
                wrong_num += 1
                #print(idx, "Error! old_title=[{}], \tnew_title=[{}], year=[{}]".format(origin_title, title, year))
                df.at[idx, 'title'] = origin_title
                year_l.append(np.nan)

        # creat a new column for the year information
        assert len(year_l) == len(df)
        df['year'] = year_l

        print("\n----> {}: #row={}, columns={}".format(file_name, len(df), df.columns),  "\n", df.head(5))
        return df

    def read_link(self, file_name):
        df = pd.read_csv(file_name, header=0, sep=',', error_bad_lines=True, low_memory=False, dtype=str)
                         #index_col='movieId')
        for idx, row in df.iterrows():
            ## convert IMDB id from ####### to tt#######
            df.at[idx, 'imdbId'] = 'tt'+row['imdbId']

        print("\n----> {}: #row={}, columns={}".format(file_name, len(df), df.columns),  "\n", df.head(5))
        return df



if __name__ == '__main__':
    d = MovieLenData(data_name='ml-25m')