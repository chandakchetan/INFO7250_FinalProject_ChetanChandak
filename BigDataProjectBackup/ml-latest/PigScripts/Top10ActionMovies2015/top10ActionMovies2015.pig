movieRatings = LOAD '/home/chetanchandak/Documents/ml-latest/JoinedTables.csv' using PigStorage(',') as (userId:int, movieId:int, ratings:float, movieName:chararray, year:chararray, genre:chararray);
year2015 = filter movieRatings by $4=='2015';

-- replace '|' with '*' to tokenize
movieGenreCleaning = FOREACH year2015 GENERATE $3, $4, $2, REPLACE($5,'\\|','*') as genre;
moviesGenre = FOREACH movieGenreCleaning GENERATE $0, $1, $2, FLATTEN(TOKENIZE($3)) as genre;
actionGenre = FILTER moviesGenre by $3=='Action';
groupByTitle =  GROUP actionGenre by $0;
averageRatingOfEachActionMovie = FOREACH groupByTitle GENERATE $0 as movieName, AVG($1.ratings) as averageMovieRating;
orderActionMoviesByRatingsDesc = ORDER averageRatingOfEachActionMovie by averageMovieRating DESC;
top10ActionMovies = LIMIT orderActionMoviesByRatingsDesc 10;

STORE top10ActionMovies into '/home/chetanchandak/Documents/Top10ActionMovies2015' using PigStorage();

