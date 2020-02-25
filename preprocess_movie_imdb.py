from urllib import request
import subprocess
import pickle
import math
from collections import Counter
import pandas as pd
import time
import csv
import os
import numpy as np


import sys
maxInt = sys.maxsize
decrement = True
while decrement:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.s
    decrement = False
    try:
        csv.field_size_limit(maxInt)
    except OverflowError:
        maxInt = int(maxInt/10)
        decrement = True

GENRES_ML_100K =\
    ['unknown', 'Action', 'Adventure', 'Animation',
     'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy',
     'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi',
     'Thriller', 'War', 'Western']
GENRES_ML_1M = GENRES_ML_100K[1:]
GENRES_ML_10M = GENRES_ML_100K + ['IMAX']

NAME = "ml-10m"
READ_DATASET_DIR = os.path.realpath(os.path.join(os.path.abspath(__file__), '..', "movielens", "raw", NAME))
SAVE_DATASET_DIR =os.path.realpath(os.path.join(os.path.abspath(__file__), '..', "movielens", "statistics", NAME))
if not os.path.isdir(SAVE_DATASET_DIR):
    os.mkdir(SAVE_DATASET_DIR)

IMDB_DIR = "_IMDB"
MOVIE_DIR =os.path.realpath(os.path.join(os.path.abspath(__file__), '..', "movielens", "statistics", NAME))


def _load_manually_fixed_dic():
    ### loading the manually fixed title info
    f_manually_fixed_title_name = os.path.join(SAVE_DATASET_DIR, "..", "manually_fixed_title_name")
    if not os.path.isfile(f_manually_fixed_title_name):
        data_file = request.urlopen("https://www.dropbox.com/s/k1x4vf7vpi9xo6j/manually_fixed_title_name?dl=1")
        with open(f_manually_fixed_title_name, 'wb') as output:
            output.write(data_file.read())
    manually_fixed_title_name = pd.read_csv(f_manually_fixed_title_name, sep="|", header=0, dtype=str, encoding="utf-8")
    wrong2correct_name_dic = {}
    for i in range(manually_fixed_title_name.shape[0]):
        _wrong_title_name = manually_fixed_title_name.iloc[i]["wrong_title_name"].lower()
        _correct_title_name = manually_fixed_title_name.iloc[i]["correct_title_name"].lower()
        if _wrong_title_name in wrong2correct_name_dic:
            assert _correct_title_name == wrong2correct_name_dic[_wrong_title_name]
            print(_wrong_title_name)
        else:
            wrong2correct_name_dic[_wrong_title_name] = _correct_title_name
    print("{} manually fixed title names".format(len(wrong2correct_name_dic)))
    return wrong2correct_name_dic

def _adjust_title_name(title_name, tokens_l_space=["the", "a", "an", "la", "le", "les", "il", "das", "el", "da", "los"],
                      tokens_l_no_space=["l'"], tokens_l_no2space=["the"]):
    for token in tokens_l_space:
        token_len = 2 + len(token)
        if title_name[-token_len:] == ", " + token:
            title_name = token + " " + title_name[: -token_len]
            break
    for token in tokens_l_no_space:
        token_len = 2 + len(token)
        if title_name[-token_len:] == ", " + token:
            title_name = token + title_name[: -token_len]
            break
    for token in tokens_l_no2space:
        token_len = 1 + len(token)
        if title_name[-token_len:] == "," + token:
            title_name = token + " " + title_name[: -token_len]
            break
    return title_name

def _extract_alias(title_name):
    split_token = " ("
    aka_token = "a.k.a. "
    if title_name.find(split_token) > 0:
        new_title = title_name[:title_name.find(split_token)]
        alias = title_name[title_name.find(split_token) + len(split_token):]
        assert alias[-1] == ")"
        alias = alias[:-1]
        # print("alias", alias)
        alias_str = ""
        while alias.find(") (") > 0:
            # print(alias)
            one_alias = alias[:alias.find(") (")]
            one_alias = one_alias[len(aka_token):] if aka_token == one_alias[:len(aka_token)] else one_alias
            alias_str += one_alias + "/"
            alias = alias[alias.find(") (") + len(") ("):]
        alias = alias[len(aka_token):] if aka_token == alias[:len(aka_token)] else alias
        alias = _adjust_title_name(alias)
        alias_str += alias
        # print("alias_str", alias_str)
        return [new_title, alias_str]
    else:
        return [title_name, ""]

def _manually_fix_year(title_name, year):
    ### TODO manually fix for repeated tiles
    if title_name == "Shall We Dance?".lower() and year == "1937":
        title_name = "Shall We Dance".lower()
    elif title_name == "Shall We Dance?".lower() and year == "1996":
        title_name = "Shall we dansu?".lower()
    elif title_name == "steamboat willie".lower() and year == "1940":
        year = "1928"
    elif title_name == "la nuit fantastique".lower() and year == "1949":
        year = "1942"
    elif title_name == "blind chance".lower() and year == "1981":
        year = "1987"
    elif title_name == "a room with a view".lower() and year == "1986":
        year = "1985"
    elif title_name == "a dog's life".lower() and year == "1920":
        year = "1918"
    elif title_name == "dna".lower() and year == "1997":
        year = "1996"
    elif title_name == "4".lower() and year == "2005":
        year = "2004"
    elif title_name == "300".lower() and year == "2007":
        year = "2006"
    elif title_name == "boot camp".lower() and year == "2007":
        year = "2008"
    elif title_name == "jubilee".lower() and year == "1977":
        year = "1978"
    elif title_name == "the reckoning".lower() and year == "2004":
        year = "2002"

    return title_name, year

def process_movielens():
    if NAME == "ml-100k":
        movielens_title_path = os.path.join(READ_DATASET_DIR, "u.item")
        movie_info = pd.read_csv(movielens_title_path, sep='|', header=None,  dtype=str, encoding='latin-1',
                                 names=['id', 'title', 'release_date', 'video_release_date', 'url'] + GENRES_ML_100K)
    elif NAME == "ml-1m" :
        movielens_title_path = os.path.join(READ_DATASET_DIR, "movies.dat")
        movie_info = pd.read_csv(movielens_title_path, sep='::', header=None, dtype=str,  encoding='latin-1',
                                 names=['id', 'title', 'genres'])
    elif NAME == "ml-10m":
        movielens_title_path = os.path.join(READ_DATASET_DIR, "movies.dat")
        movie_info = pd.read_csv(movielens_title_path, sep='::', header=None, dtype=str, encoding="utf",
                                 names=['id', 'title', 'genres'])
    else:
        raise NotImplementedError

    wrong2correct_name_dic = _load_manually_fixed_dic()

    new_cols = []
    _year_dic = {}
    ml_id2l_title_year_dic = {} ### {movie_id: [[name, year], ...] }
    _duplicated_titles_l = []
    _ml_titles_year2id_dic = {} ### {(title_name, year): movie_id }
    for i in range(movie_info.shape[0]):
        one_row = movie_info.iloc[i]
        movie_id = one_row["id"]
        old_title = one_row["title"].strip(" ")
        if old_title != 'unknown':
            ### Extract and revise the title name and the release year
            ### Deal with the special case ...
            if " (V)" in old_title[-4:]:
                old_title = old_title[:-4]

            year = old_title[-5:-1]
            title_name_orig = old_title[:-6].strip(" ").lower()
            title_name, alias_str = _extract_alias(title_name_orig)

            ### preprocessing the title name and year
            revised_title_name = _adjust_title_name(title_name)
            if revised_title_name in wrong2correct_name_dic:
                revised_title_name = wrong2correct_name_dic[revised_title_name].lower()
            revised_title_name, year = _manually_fix_year(revised_title_name, year)
            assert int(year) > 1000

            if int(year) not in _year_dic:
                _year_dic[int(year)] = True
            new_cols.append([movie_id, old_title, year, revised_title_name, alias_str])

            ### Start store the movielens data
            key = (revised_title_name, year)
            ml_id2l_title_year_dic[movie_id] = [key]

            ### GET the duplicated titles
            if key in _ml_titles_year2id_dic:
                _duplicated_titles_l.append(key)
                _ml_titles_year2id_dic[key].append(movie_id)
            else:
                _ml_titles_year2id_dic[key] = [movie_id]

            for als in alias_str.split("/"):
                if als != "nan" and len(als) > 0:
                    key = (als, year)
                    ml_id2l_title_year_dic[movie_id].append(key)
                    ### GET the duplicated titles
                    if key in _ml_titles_year2id_dic:
                        _duplicated_titles_l.append(key)
                        _ml_titles_year2id_dic[key].append(movie_id)
                    else:
                        _ml_titles_year2id_dic[key] = [movie_id]
        else:
            new_cols.append([movie_id, old_title, "0000", old_title, ""])


    ##########################################################################################
    ##########################################################################################
    year_l = _year_dic.keys()
    print("There are {} different year time, with the min {} and max {}".format(len(year_l),
                                                                                       min(year_l), max(year_l)))
    new_np = np.vstack(new_cols)
    print("Saving the preprocessing titles as [id, old_title, year, new_title, alias]")
    movielens_pd = pd.DataFrame(new_np, columns=["id", "old_title", "year", "new_title", "alias"])

    movielens_pd.to_csv(os.path.join(SAVE_DATASET_DIR, "preprocessed_titles"), sep='|', index=False)
    ## movielens_pd = pd.read_csv(os.path.join(MOVIE_DIR, "preprocessed_titles"), sep='|', header=0, dtype=str, encoding='utf-8', engine='python')

    ##########################################################################################
    ##########################################################################################
    ### save the duplicated titles for reference
    print("{} duplicated movies".format(len(_duplicated_titles_l)))
    f_id = open(os.path.join(MOVIE_DIR, "duplicated_movie_pairs"), "w")
    f_name = open(os.path.join(MOVIE_DIR, "duplicated_items.txt"), "w")
    for duplicate_name_key in _duplicated_titles_l:
        duplicated_ids = []
        for i in range(movielens_pd.shape[0]):
            one_row = movielens_pd.iloc[i]
            if one_row["new_title"] == duplicate_name_key[0] and one_row["year"] == duplicate_name_key[1]:
                f_name.write("|".join(one_row.values.astype(str).tolist()) + "\n")
                duplicated_ids.append(one_row["id"])
        f_name.write("\n")
        f_id.write("\t".join(duplicated_ids) + "\n")

    ##########################################################################################
    ##########################################################################################
    ### processing genre dic
    if NAME == "ml-100k":
        genres_np = movie_info[GENRES_ML_100K].values.astype(np.int)
        genres_l = []
        for i in range(genres_np.shape[0]):
            one_genre_l = []
            for j in range(genres_np.shape[1]):
                if genres_np[i][j] == 1:
                    one_genre_l.append(GENRES_ML_100K[j].lower())
            genres_l.append(one_genre_l)
    elif NAME == "ml-1m" or NAME == "ml-10m":
        genres = movie_info["genres"]
        genres_l = []
        for i in range(genres.shape[0]):
            genres_vec = genres[i].lower().split("|")
            genres_l.append(genres_vec)
    else:
        raise NotImplementedError
    assert len(genres_l) == movie_info.shape[0]
    ml_id21_genre_dic = {}
    for i in range(movie_info.shape[0]):
        ml_id21_genre_dic[movie_info.iloc[i]["id"]] = genres_l[i]

    return ml_id2l_title_year_dic, ml_id21_genre_dic

###################################################################################################
###################################################################################################
def _download_imdb(flag_overwrite=False):
    if os.path.isdir(IMDB_DIR) is False:
        os.mkdir(IMDB_DIR)
    DOWNLOAD_INFO = [('title_basics.tsv.gz', 'https://datasets.imdbws.com/title.basics.tsv.gz'),
                     ('title_akas.tsv.gz', 'https://datasets.imdbws.com/title.akas.tsv.gz'),
                     ('title_crew.tsv.gz', 'https://datasets.imdbws.com/title.crew.tsv.gz'),
                     ('name_basics.tsv.gz', 'https://datasets.imdbws.com/name.basics.tsv.gz')]
    for save_name, url in DOWNLOAD_INFO:
        if os.path.isfile(os.path.join(IMDB_DIR, save_name[:-3])) or not flag_overwrite:
            print("Found {}, Skip".format(os.path.join(IMDB_DIR, save_name)))
        else:
            data_file = request.urlopen(url)
            with open(os.path.join(IMDB_DIR, save_name), 'wb') as output:
                output.write(data_file.read())
            subprocess.call(['gunzip', '{}/{}'.format(IMDB_DIR, save_name)])
            subprocess.call(['rm', '{}/{}'.format(IMDB_DIR, save_name)])

def read_imdb2dic():
    _download_imdb()
    titles2id_dic = {}
    titles2year_dic = {}
    _id2year_dic = {}
    id2info_dic = {}
    id2genre_dic = {}
    with open(os.path.join(IMDB_DIR, "title_basics.tsv"), newline='', encoding='utf-8') as csvfile:
        IMDB_title_name = csv.reader(csvfile, delimiter='\t')
        for row in IMDB_title_name:
            str_id = row[0]
            title_type = row[1].lower()
            title1 = row[2].lower()
            title2 = row[3].lower()
            assert "\n" not in title1 and "\n" not in title2
            start_year = row[5]
            end_year = row[6]

            start_year = None if start_year == '\\N' else start_year
            end_year = None if end_year == '\\N' else end_year
            if start_year is not None and len(row) == 9:
                if str_id not in _id2year_dic:
                    _id2year_dic[str_id] = start_year
                if str_id not in id2info_dic:
                    id2info_dic[str_id] = [(start_year, end_year), title_type]
                if str_id not in id2genre_dic:
                    id2genre_dic[str_id] = row[8].lower().split(",")

                if title1 not in titles2id_dic:
                    titles2id_dic[title1] = {}
                    titles2id_dic[title1][str_id] = start_year
                    titles2year_dic[title1] = {}
                    titles2year_dic[title1][start_year] = [str_id]
                else:
                    if str_id not in titles2id_dic[title1]:
                        titles2id_dic[title1][str_id] = start_year
                    if start_year not in titles2year_dic[title1]:
                        titles2year_dic[title1][start_year] = [str_id]
                    else:
                        titles2year_dic[title1][start_year].append(str_id)

                if title2 != title1:
                    if title2 not in titles2id_dic:
                        titles2id_dic[title2] = {}
                        titles2id_dic[title2][str_id] = start_year
                        titles2year_dic[title2] = {}
                        titles2year_dic[title2][start_year] = [str_id]
                    else:
                        if str_id not in titles2id_dic[title2]:
                            titles2id_dic[title2][str_id] = start_year
                        if start_year not in titles2year_dic[title2]:
                            titles2year_dic[title2][start_year] = [str_id]
                        else:
                            titles2year_dic[title2][start_year].append(str_id)
                else:
                    continue
            else:
                continue

    with open(os.path.join(IMDB_DIR, "title_akas.tsv"), newline='', encoding='utf-8') as csvfile2:
        IMDB_akas_name = csv.reader(csvfile2, delimiter="\t")
        for row in IMDB_akas_name:
            str_id = row[0]
            title3 = row[2].lower()
            if "\n" in title3:
                print("len(title3)", len(title3))
                continue
            assert "\n" not in title3
            if str_id in _id2year_dic:
                year = _id2year_dic[str_id]
                if title3 not in titles2id_dic:
                    titles2id_dic[title3] = {}
                    titles2id_dic[title3][str_id] = year
                    titles2year_dic[title3] = {}
                    titles2year_dic[title3][year] = [str_id]
                else:
                    if str_id not in titles2id_dic[title3]:
                        titles2id_dic[title3][str_id] = year
                    if year not in titles2year_dic[title3]:
                        titles2year_dic[title3][year] = [str_id]
                    else:
                        titles2year_dic[title3][year].append(str_id)
            else:
                continue

    print("#title name: {}".format(len(titles2id_dic)))
    print("#movie id: {}".format(len(id2info_dic)))
    with open(os.path.join(IMDB_DIR,'_title_name2idsdic_dic.pkl'), 'wb') as f:
        pickle.dump(titles2id_dic, f)
    with open(os.path.join(IMDB_DIR,'_title_name2yeardic_dic.pkl'), 'wb') as f:
        pickle.dump(titles2year_dic, f)
    with open(os.path.join(IMDB_DIR, '_id2info_dic.pkl'), 'wb') as f:
        pickle.dump(id2info_dic, f)
    with open(os.path.join(IMDB_DIR, '_id2genre_dic.pkl'), 'wb') as f:
        pickle.dump(id2genre_dic, f)
    ###################################################################################
    ###################################################################################

    id2l_director_dic = {}
    id2l_writer_dic = {}
    with open(os.path.join(IMDB_DIR, "title_crew.tsv"), newline='', encoding='utf-8') as csvfile:
        file_rows = csv.reader(csvfile, delimiter='\t')
        for row in file_rows:
            id = row[0]
            director_str = row[1]
            writer_str = row[2]

            if id in id2l_director_dic:
                print(id, id2l_director_dic[id])
            else:
                if director_str != "\\N" and len(director_str) > 2:
                    director_vec = director_str.split(",")
                    id2l_director_dic[id] = director_vec

            if id in id2l_writer_dic:
                print(id, id2l_writer_dic[id])
            else:
                if writer_str != "\\N" and len(writer_str) > 2:
                    writer_vec = writer_str.split(",")
                    id2l_writer_dic[id] = writer_vec
    with open(os.path.join(IMDB_DIR, '_id2director_dic.pkl'), 'wb') as f:
        pickle.dump(id2l_director_dic, f)
    with open(os.path.join(IMDB_DIR, '_id2writer_dic.pkl'), 'wb') as f:
        pickle.dump(id2l_writer_dic, f)
    ###################################################################################
    ###################################################################################

    people_id2name_dic = {}
    with open(os.path.join(IMDB_DIR, "name_basics.tsv"), newline='', encoding='utf-8') as csvfile:
        file_rows = csv.reader(csvfile, delimiter='\t')
        for row in file_rows:
            id = row[0]
            name = row[1]
            if id in people_id2name_dic:
                print(id, people_id2name_dic[id])
            else:
                people_id2name_dic[id] = name
    with open(os.path.join(IMDB_DIR, '_people_id2name_dic.pkl'), 'wb') as f:
        pickle.dump(people_id2name_dic, f)

    print("IMDb dics generated ...")
    return titles2id_dic, titles2year_dic, id2info_dic, id2genre_dic, \
           id2l_director_dic, id2l_writer_dic, people_id2name_dic

def load_imdb_dics():
    with open(os.path.join(IMDB_DIR,'_title_name2idsdic_dic.pkl'), 'rb') as f:
        titles2id_dic = pickle.load(f)
    with open(os.path.join(IMDB_DIR,'_title_name2yeardic_dic.pkl'), 'rb') as f:
        titles2year_dic = pickle.load(f)
    with open(os.path.join(IMDB_DIR, '_id2info_dic.pkl'), 'rb') as f:
        id2info_dic = pickle.load(f)
    with open(os.path.join(IMDB_DIR, '_id2genre_dic.pkl'), 'rb') as f:
        id2genre_dic = pickle.load(f)

    with open(os.path.join(IMDB_DIR,'_id2director_dic.pkl'), 'rb') as f:
        id2l_director_dic = pickle.load(f)
    with open(os.path.join(IMDB_DIR,'_id2writer_dic.pkl'), 'rb') as f:
        id2l_writer_dic = pickle.load(f)
    with open(os.path.join(IMDB_DIR,'_people_id2name_dic.pkl'), 'rb') as f:
        people_id2name_dic = pickle.load(f)
    print("IMDb dics loaded ...")

    return titles2id_dic, titles2year_dic, id2info_dic, id2genre_dic, \
           id2l_director_dic, id2l_writer_dic, people_id2name_dic
###################################################################################################
###################################################################################################

def _counter_cosine_similarity(l1, l2):
    c1 = Counter(l1)
    c2 = Counter(l2)
    terms = set(c1).union(c2)
    dotprod = sum(c1.get(k, 0) * c2.get(k, 0) for k in terms)
    magA = math.sqrt(sum(c1.get(k, 0)**2 for k in terms))
    magB = math.sqrt(sum(c2.get(k, 0)**2 for k in terms))
    return dotprod / (magA * magB)

def _gen_info(ml_id, ml_id2l_title_year_dic, ml_id21_genre_dic, imdb_id_l, id2info_dic, id2genre_dic):
    info = '\n{} '.format(ml_id)
    for ml_name, year in ml_id2l_title_year_dic[ml_id]:
        info += "\t{}: year={}".format(ml_name, year)
    info += "\t"
    for gnr in ml_id21_genre_dic[ml_id]:
        info += "{} ".format(gnr)
    info += "\n"
    for imdb_id in imdb_id_l:
        info += '\t{}: year=({},{}), title type={}'.format(imdb_id, id2info_dic[imdb_id][0][0],
                                                           id2info_dic[imdb_id][0][1],
                                                           id2info_dic[imdb_id][1])
        info += "\t\tgenre="
        for gnr in id2genre_dic[imdb_id]:
            info += "{} ".format(gnr)
        score = _counter_cosine_similarity(ml_id21_genre_dic[ml_id], id2genre_dic[imdb_id])
        info += "\t\tScore:{}\n".format(score)
    return info

def _l2pd(orig_l, columns):
    new_l = []
    for k, v_l in orig_l:
        for v in v_l:
            new_l.append([int(k), v])
    new_pd = pd.DataFrame(new_l, columns=columns)
    return new_pd

def _gen_unique_info(orig_pd, use_colum_name, people_id2name_dic, columns):
    unique_ = orig_pd[use_colum_name].drop_duplicates()
    unique_ = unique_.sort_values()
    unique_l = [[id, people_id2name_dic[id]] for id in unique_.values]
    unique_pd = pd.DataFrame(unique_l, columns=columns)
    return unique_pd


def match(ml_id2l_title_year_dic, ml_id21_genre_dic,
          titles2id_dic, titles2year_dic, id2info_dic, id2genre_dic, id2l_director_dic, id2l_writer_dic, people_id2name_dic,
          year_diff_thres, chosen_title_type_l, COS_SIM_THRES):
    NUM_MATCHED_TITLE_YEAR = 0
    NUM_UNMATCHED_TITLE = 0
    NUM_UNMATCHED_YEAR = 0
    unmatched_year_l = [] ## ["ml_id", "title", "year", "imdb_ids"]
    unmatched_title_l = []
    _matched_mlid2l_imdbids_dic = {}
    for ml_id in ml_id2l_title_year_dic:
        FLAG_TITLE_MATCHED = False
        FLAG_TITLE_YEAR_MATCHED = False
        _one_unmatched_year_l = []
        ### Use the tile and alias name and year to find the candidates
        for ml_name, ml_year in ml_id2l_title_year_dic[ml_id]:
            if ml_name in titles2year_dic:
                FLAG_TITLE_MATCHED = True
                if ml_year in titles2year_dic[ml_name]:
                    FLAG_TITLE_YEAR_MATCHED = True
                    if ml_id in _matched_mlid2l_imdbids_dic:
                        _matched_mlid2l_imdbids_dic[ml_id] += titles2year_dic[ml_name][ml_year]
                    else:
                        _matched_mlid2l_imdbids_dic[ml_id] = titles2year_dic[ml_name][ml_year]
                else:
                    ### save for fuzzy match
                    _one_unmatched_year_l.append([ml_id, ml_name, ml_year, titles2id_dic[ml_name].keys()])

        if FLAG_TITLE_MATCHED == True:
            if FLAG_TITLE_YEAR_MATCHED == True:
                NUM_MATCHED_TITLE_YEAR += 1
            else:
                ### start fuzzy match
                FLAG_FUZZY_YEAR_MATCHED = False
                matched_imdb_ids_l = []
                for ml_id, ml_name, ml_year, imdb_id_l in _one_unmatched_year_l:
                    for imdb_id in imdb_id_l:
                        id_info = id2info_dic[imdb_id]
                        start_year = id_info[0][0]
                        end_year = id_info[0][1]
                        if (end_year is not None) and (int(ml_year) >= int(start_year)) and (int(ml_year) <= int(end_year)):
                            FLAG_FUZZY_YEAR_MATCHED = True
                            matched_imdb_ids_l.append(imdb_id)
                        elif (start_year is not None) and (abs(int(start_year) - int(ml_year)) <= year_diff_thres):
                            FLAG_FUZZY_YEAR_MATCHED = True
                            matched_imdb_ids_l.append(imdb_id)

                if FLAG_FUZZY_YEAR_MATCHED:
                    NUM_MATCHED_TITLE_YEAR += 1
                    if ml_id in _matched_mlid2l_imdbids_dic:
                        _matched_mlid2l_imdbids_dic[ml_id] += matched_imdb_ids_l
                    else:
                        _matched_mlid2l_imdbids_dic[ml_id] = matched_imdb_ids_l
                else:
                    NUM_UNMATCHED_YEAR += 1
                    for i in range(len(_one_unmatched_year_l)):
                        _one_unmatched_year_l[i][-1] = "/".join(_one_unmatched_year_l[i][-1])
                    unmatched_year_l += _one_unmatched_year_l
        else:
            NUM_UNMATCHED_TITLE += 1
            unmatched_title_l.append([ml_id, "||".join([ml_name + "-->" + ml_year
                                                        for ml_name, ml_year in ml_id2l_title_year_dic[ml_id]])])
    assert NUM_UNMATCHED_TITLE == len(unmatched_title_l)
    assert NUM_UNMATCHED_YEAR == len(unmatched_year_l)
    assert NUM_MATCHED_TITLE_YEAR == len(_matched_mlid2l_imdbids_dic)
    print("\t# matched name &year: {}/{}".format(NUM_MATCHED_TITLE_YEAR, len(ml_id2l_title_year_dic)))
    print("\t# unmatched title: {}/{}".format(NUM_UNMATCHED_TITLE, len(ml_id2l_title_year_dic)))
    print("\t# unmatched year: {}/{}".format(NUM_UNMATCHED_YEAR, len(ml_id2l_title_year_dic)))
    ### save for reference
    matched_title_year_l = [[k, "/".join(list(set(v)))] for k,v in _matched_mlid2l_imdbids_dic.items()]
    matched_title_year_pd = pd.DataFrame(matched_title_year_l, columns=["ml_id", "imdb_id"])
    matched_title_year_pd = matched_title_year_pd.drop_duplicates()
    matched_title_year_pd.to_csv(os.path.join(MOVIE_DIR, 'matched_title.pd'), sep="\t", index=False)
    ## matched_title_year_pd =  pd.read_csv(os.path.join(SAVE_DATASET_DIR, 'matched_title.pd'), sep='\t', header=0, dtype=str, encoding='utf-8', engine='python')
    unmatched_title_pd = pd.DataFrame(unmatched_title_l, columns=["ml_id", "info"])
    unmatched_title_pd = unmatched_title_pd.drop_duplicates()
    unmatched_title_pd.to_csv(os.path.join(MOVIE_DIR, 'unmatched_title.pd'), sep="\t", index=False)

    unmatched_year_pd = pd.DataFrame(unmatched_year_l, columns=["ml_id", "title", "year", "imdb_ids"])
    unmatched_year_pd.to_csv(os.path.join(MOVIE_DIR, 'unmatched_year.pd'), sep="\t", index=False)

    ###################################################################################################
    ###################################################################################################
    print("Start matching movie with director/writer ....")
    movie_id2l_directors_l = []
    movie_id2l_writers_l = []
    NUM_NOT_MATCHED_TYPE = 0
    NUM_NOT_FILTERED = 0
    NUM_FOUND = 0
    for ml_id, matched_imdb_id_l in _matched_mlid2l_imdbids_dic.items():
        if len(matched_imdb_id_l) > 1:
            ### Start filtering the movies with the most accurate one ...
            ### Successively check the title types
            _title_type_l = [id2info_dic[imdb_id][1] for imdb_id in matched_imdb_id_l]
            FLAG_TYPE_FOUND = False
            for title_type_token in chosen_title_type_l: # ["movie", "tvmovie", "tvminiseries", "video"]
                if title_type_token in _title_type_l:
                    FLAG_TYPE_FOUND = True
                    _mapped_director_info = []
                    _mapped_writer_info = []
                    _mapped_imdb_info_dic = {}
                    for imdb_id in matched_imdb_id_l:
                        if id2info_dic[imdb_id][1] == title_type_token:
                            _mapped_imdb_info_dic[imdb_id] = id2info_dic[imdb_id]
                            if imdb_id in id2l_director_dic:
                                _mapped_director_info.append([ml_id, id2l_director_dic[imdb_id]])
                            if imdb_id in id2l_writer_dic:
                                _mapped_writer_info.append([ml_id, id2l_writer_dic[imdb_id]])

                    if len(_mapped_imdb_info_dic) == 1:
                        NUM_FOUND += 1
                        movie_id2l_directors_l += _mapped_director_info
                        movie_id2l_writers_l += _mapped_writer_info
                    elif len(_mapped_imdb_info_dic) > 1:
                        ### Use genres to map movies
                        max_score = COS_SIM_THRES
                        for imdb_id, _ in _mapped_imdb_info_dic.items():
                            sim_score = _counter_cosine_similarity(ml_id21_genre_dic[ml_id], id2genre_dic[imdb_id])
                            if sim_score > max(max_score, COS_SIM_THRES):
                                max_score = sim_score
                                _mapped_imdb_id = imdb_id
                        if max_score > 0.3:
                            if _mapped_imdb_id in id2l_director_dic:
                                movie_id2l_directors_l.append([ml_id, id2l_director_dic[_mapped_imdb_id]])
                            if _mapped_imdb_id in id2l_writer_dic:
                                movie_id2l_writers_l.append([ml_id, id2l_writer_dic[_mapped_imdb_id]])
                        else:
                            NUM_NOT_FILTERED += 1
                            print(_gen_info(ml_id, ml_id2l_title_year_dic, ml_id21_genre_dic, imdb_id_l,
                                                   id2info_dic, id2genre_dic))
                    break
            if FLAG_TYPE_FOUND == False:
                NUM_NOT_MATCHED_TYPE += 1
                print(_gen_info(ml_id, ml_id2l_title_year_dic, ml_id21_genre_dic, imdb_id_l,
                                       id2info_dic, id2genre_dic))
        else:
            NUM_FOUND += 1
            imdb_id = matched_imdb_id_l[0]
            assert len(imdb_id) == 9
            if imdb_id in id2l_director_dic:
                movie_id2l_directors_l.append([ml_id, id2l_director_dic[imdb_id]])
            if imdb_id in id2l_writer_dic:
                movie_id2l_writers_l.append([ml_id, id2l_writer_dic[imdb_id]])

    print("# found{}/{}, dropped: {} (not matched) + {} (>2 titles)".format(NUM_FOUND, len(ml_id2l_title_year_dic),
                                                                                   NUM_NOT_MATCHED_TYPE, NUM_NOT_FILTERED))
    print("# mapped (director): {}, # mapped (writer): {}".format(len(movie_id2l_directors_l),
                                                                            len(movie_id2l_writers_l)))

    movie_id2director_id_pd = _l2pd(movie_id2l_directors_l, ["movie_id", "director_id"])
    movie_id2director_id_pd = movie_id2director_id_pd.drop_duplicates()
    movie_id2director_id_pd = movie_id2director_id_pd.sort_values(by=["movie_id"])
    movie_id2director_id_pd.to_csv(os.path.join(READ_DATASET_DIR, 'director.map'), sep="\t", index=False)

    movie_id2writer_id_pd = _l2pd(movie_id2l_writers_l, ["movie_id", "writer_id"])
    movie_id2writer_id_pd = movie_id2writer_id_pd.drop_duplicates()
    movie_id2writer_id_pd = movie_id2writer_id_pd.sort_values(by=["movie_id"])
    movie_id2writer_id_pd.to_csv(os.path.join(READ_DATASET_DIR, 'writer.map'), sep="\t", index=False)

    unique_director_name_pd = _gen_unique_info(movie_id2director_id_pd, "director_id", people_id2name_dic,
                                              ["director_id", "name"])
    unique_director_name_pd.to_csv(os.path.join(READ_DATASET_DIR, 'director.dat'), sep="\t", index=False)

    unique_writer_name_pd = _gen_unique_info(movie_id2writer_id_pd, "writer_id", people_id2name_dic,
                                              ["writer_id", "name"])
    unique_writer_name_pd.to_csv(os.path.join(READ_DATASET_DIR, 'writer.dat'), sep="\t", index=False)


if __name__ == '__main__':
    print("==================================================")

    # titles2id_dic, titles2year_dic, id2info_dic, id2genre_dic,\
    # id2l_director_dic, id2l_writer_dic, people_id2name_dic = read_imdb2dic()
    titles2id_dic, titles2year_dic, id2info_dic, id2genre_dic, \
    id2l_director_dic, id2l_writer_dic, people_id2name_dic = load_imdb_dics()

    ml_id2l_title_year_dic, ml_id21_genre_dic = process_movielens()
    ### START MATCHING
    print("Start matching ...")
    start = time.clock()
    match(ml_id2l_title_year_dic, ml_id21_genre_dic, titles2id_dic, titles2year_dic, id2info_dic,
          id2genre_dic, id2l_director_dic, id2l_writer_dic, people_id2name_dic,
          year_diff_thres=4, chosen_title_type_l=["movie", "tvmovie", "tvminiseries", "video"], COS_SIM_THRES=0.3)
    print("Time for matching {}s".format((time.clock()- start)*3600))
