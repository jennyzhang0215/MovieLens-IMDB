# MovieLens-IMDB

How to match the MovieLens dataset and the IMDB dataset?

### ML-100K, ML-1M, MK-10M 
For these three datasets, we need to match the movies using the title name and release year. As shown in the README in ML-10M:
Movie titles, by policy, should be entered identically to those found in IMDB, including year of release. However, they are entered manually, so errors and inconsistencies may exist. 

So in order to fix the inconsistencies, we manually match the movies in MovieLens, where they cannot be directly found in IMDB according to the original title name. For example, the title name in MovieLens is `jungle2jungle`, wheaeas the title name in IMDB is `jungle 2 jungle`. The manually fixed title inconsistencies are in `movielens/statistics/manually_fixed_title_name`. 

We provide a script for your convenience to match the directors and the writers for each movie.

Step1: Download the MovieLens dataset (https://grouplens.org/datasets/movielens/) and save them in `movielens/raw/{}`.format(`ML-100K`), if you use the ML-100K dataset. Download the IMDB dataset (https://datasets.imdbws.com/) and save them in `_IMDB/*` and then unzip all the files.

Step2: Run the script
```bash
python preprocess_movie_imdb.py
```

You can obtain a network schema graph like this:

![network_schema](https://github.com/jennyzhang0215/MovieLens-IMDB/blob/master/network_schema_movielens_5.pngg)

### ML-20M

For this dataset,it already has the links for other sources. As indicated in the README file (http://files.grouplens.org/datasets/movielens/ml-20m-README.html): Identifiers that can be used to link to other sources of movie data are contained in the file links.csv. Each line of this file after the header row represents one movie, and has the following format: movieId,imdbId,tmdbId.

But these identifiers have not been checked by us and we don't know whether there exists inconsistencies or not.


