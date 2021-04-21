import pandas as pd
import argparse
import os
import numpy as np
from utils.load_IMDb import IMDbData
from utils.load_MovieLens import MovieLenData

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')

def parse_args():
    parser = argparse.ArgumentParser('Parse arguments')
    parser.add_argument('--data_name', type=str, default="ml-25m",
                        help='the data name for preprocessing')
    args, unparsed = parser.parse_known_args()
    # print arguments
    print('args:')
    for arg in vars(args):
        print(arg, ': ', getattr(args, arg))

    return args

def merge_cat(df1, df2, save_dir):
    ## add the category information
    ## ==> columns += ['titleType', 'primaryTitle', 'originalTitle', 'isAdult',
    ##                'startYear', 'endYear', 'runtimeMinutes', 'genres']
    ## the original columns of df2 = ['tconst', 'ordering', 'nconst', 'category', 'job', 'characters']
    tmp_dd = df1['tconst'].to_frame().merge(df2, how='left', on='tconst')
    print('\n\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print("The TMPT data, len={}, columns={}\n".format(len(tmp_dd), tmp_dd.columns), tmp_dd.head(10))

    ## process the nconst mapping
    nconsts = tmp_dd['nconst'].drop_duplicates(keep='first').reset_index(drop=True)
    nconst_maps = nconsts.to_frame().merge(imdb.name_basics, how='left', on='nconst')
    print('=======================================================================')
    print("The nconst_maps, len={}, columns={}\n".format(len(nconst_maps), nconst_maps.columns), nconst_maps.head(10))
    nconst_maps.to_csv(os.path.join(save_dir, 'nconst_info.csv'), sep='\t',
                       na_rep='\\N', header=True, index=False)  # ,
    # compression='gzip')

    ## process the category data
    cats = tmp_dd['category'].unique().tolist()
    cats.remove('self')
    cats.remove(np.nan)
    print('#category={} ==>'.format(len(cats)), cats)
    ## process all the category
    grouped_tmp_dd = tmp_dd \
        .groupby(['tconst', 'category']) \
        .agg({'nconst': lambda x: '|'.join(list(x))}) \
        .reset_index()
    ## merge each category data
    for cat in cats:
        new_df = grouped_tmp_dd[grouped_tmp_dd['category'] == cat]
        new_df = new_df.drop(columns=['category'])
        new_df = new_df.rename(columns={'nconst': cat})
        df1 = df1.merge(new_df, how='left', on='tconst')
    print('=======================================================================')
    print("The merged data with category, #movie={}, columns={}\n".format(len(df1), df1.columns),
          df1.head(2))
    return df1

def merge_basic(df1, df2):
    ## ==> columns += ['titleType', 'primaryTitle', 'originalTitle', 'isAdult',
    ##                'startYear', 'endYear', 'runtimeMinutes', 'genres']
    dd = df1.merge(df2, how='left', on='tconst', validate='one_to_one')
    for idx, row in dd.iterrows():
        dd.at[idx, 'genres'] = str(row['genres']).replace(',', '|')
    print('\n\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print("The merged data, #movie={}, columns={}\n".format(len(dd), dd.columns), dd.head(2))
    return dd

def merge_crews(df1, df2):
    ## ==> columns += ['directors', 'writers']
    dd = df1.merge(df2, how='left', on='tconst')
    ## convert the array dilimeter from ',' to '|'
    for idx, row in dd.iterrows():
        dd.at[idx, 'directors'] = str(row['directors']).replace(',', '|')
        dd.at[idx, 'writers'] = str(row['writers']).replace(',', '|')
    print('\n\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print("The merged data with crews, #movie={}, columns={}\n".format(len(dd), dd.columns),
          dd[['tconst', 'directors', 'writers']].head(2))
    return dd

def merge_ml(df1, df2):
    ## merge the movie id with imdb id
    ## ==> columns += ['movieId', 'title', 'genres', 'year']
    dd = df1.merge(df2, how='left', on='movieId',
                   suffixes=('_ml', '_imdb'), validate='one_to_one')
    print('\n\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print("The merged data with movielens, #movie={}, columns={}\n".format(len(dd), dd.columns), dd.head(2))
    return dd

if __name__ == '__main__':
    args = parse_args()
    out_dir = os.path.join(OUTPUT_DIR, args.data_name+"-imdb")
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)

    imdb = IMDbData()
    movielens = MovieLenData(args.data_name)

    dat = movielens.links.drop(columns=['tmdbId']) ## ['movieId', 'imdbId', 'tmdbId'] ==> ['movieId', 'tconst']
    dat.rename(columns={'imdbId': 'tconst'}, inplace=True)
    assert dat['tconst'].notna().all()
    print("\nThe movie data, #movie={}, columns={}\n".format(len(dat), dat.columns), dat.head(1))

    dat = merge_cat(dat, imdb.principals, out_dir)
    dat = merge_basic(dat, imdb.title_basics)
    dat = merge_crews(dat, imdb.crews)
    dat = merge_ml(movielens.movies, dat)
    dat.to_csv(os.path.join(out_dir, 'movie_ml_imdb.csv'), sep='\t',
                       na_rep='\\N', header=True, index=False)
