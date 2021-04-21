After run the script `process_ml-25m.py`, you can generate two files for the movie features.
Or you can download the preprocessed files [here](https://drive.google.com/file/d/1fz8WjLy0_UYioFbMirYrjhM00EYnCaWP/view?usp=sharing).
- `movie_ml_imdb.csv`
    - \#movie = 62,423
    - Columns = \[{`movieId`, `title`, `genres_ml`, `year`} (from MovieLens *movies.csv*), 
    {`tconst`, `writer`, `actor`, `director`, `producer`, `actress`,
    `cinematographer`, `composer`, `editor`, `production_designer`,
    `archive_footage`, `archive_sound`} (from IMDb *title.principals.tsv.gz*),
    {`titleType`, `primaryTitle`, `originalTitle`, `isAdult`, `startYear`,
    `endYear`, `runtimeMinutes`, `genres_imdb`} (from IMDb *title.basics.tsv.gz*), {`directors`, `writers`} (from IMDb *title.crew.tsv.gz*)]
    - `"\N"` indicates `NaN`
    - All the list data are split by `|`

- `nconst_info.csv`
    - \#nconst = 227,648
    - The filtered person information for `nconst` in `movie_ml_imdb.csv`
    - Columns = \[`nconst`, `primaryName`, `birthYear`, `deathYear`, `primaryProfession`, `knownForTitles`\]
