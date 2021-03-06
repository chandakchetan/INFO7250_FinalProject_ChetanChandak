movieRatings = LOAD '/home/chetanchandak/Documents/ml-latest/JoinedTables.csv' using PigStorage(',') as (userId:int, movieId:int, ratings:float, movieName:chararray, year:chararray, genre:chararray);
decade2000 = filter movieRatings by $4>='2001' and $4<='2010';

-- replace '|' with '*' to tokenize
movieGenreCleaning = FOREACH decade2000 GENERATE $3, $4, $2, REPLACE($5,'\\|','*') as genre;
moviesGenre = FOREACH movieGenreCleaning GENERATE $0, $1, $2, FLATTEN(TOKENIZE($3)) as genre;
comedyGenre = FILTER moviesGenre by $3=='Comedy';
groupByTitle =  GROUP comedyGenre by $0;
averageRatingOfEachActionMovie = FOREACH groupByTitle GENERATE $0 as movieName;

fileGroup= GROUP averageRatingOfEachActionMovie ALL;
numberOfComedyMoviesReleasedIn2000Decade = FOREACH fileGroup GENERATE COUNT(averageRatingOfEachActionMovie);

STORE numberOfComedyMoviesReleasedIn2000Decade into '/home/chetanchandak/Documents/NumberOfComedyMoviesReleasedIn2000Decade' using PigStorage();
