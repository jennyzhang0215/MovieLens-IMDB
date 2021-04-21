import os
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'IMDb')
NAME_BASICS_FILE = 'name.basics.tsv.gz'
TITLE_BASICS_FILE = 'title.basics.tsv.gz'
CREW_FILE = 'title.crew.tsv.gz'
PRINCIPALS_FILE = 'title.principals.tsv.gz'
EPISODE_FILE = 'title.episode.tsv.gz'
AKAS_FILE = 'title.akas.tsv.gz'
RATING_FILE = 'title.ratings.tsv.gz'

class IMDbData():
    def __init__(self):
        # name_basics_data = self.read_data(os.path.join(DATA_DIR, NAME_BASICS_FILE))

        # episode_data = self.read_data(os.path.join(DATA_DIR, CREW_FILE))
        # ratings_data = self.read_data(os.path.join(DATA_DIR, RATING_FILE))
        # akas_data = self.read_data(os.path.join(DATA_DIR, AKAS_FILE))
        pass
    @property
    def title_basics(self):
        ## [tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres]
        return self.read_data(os.path.join(DATA_DIR, TITLE_BASICS_FILE))
    @property
    def crews(self):
        ## [tconst, directors, writers]
        return self.read_data(os.path.join(DATA_DIR, CREW_FILE))
    @property
    def principals(self):
        ## [tconst, ordering, nconst, category, job characters]
        ## categories = ['writer', 'actor', 'director', 'producer', 'actress',
        ##               'cinematographer', 'composer', 'editor', 'production_designer',
        ##               'archive_footage', 'archive_sound']
        return self.read_data(os.path.join(DATA_DIR, PRINCIPALS_FILE))
    @property
    def name_basics(self):
        ##
        return self.read_data(os.path.join(DATA_DIR, NAME_BASICS_FILE))


    def read_data(self, file_name):
        df = pd.read_csv(file_name, compression='gzip', header=0, sep='\t',
                         error_bad_lines=True, low_memory=False, dtype=str)
        print("\n----> {}: #row={}, columns={}".format(file_name, len(df), df.columns),  "\n", df.head(5))

        return df

if __name__ == '__main__':
    d = IMDbData()