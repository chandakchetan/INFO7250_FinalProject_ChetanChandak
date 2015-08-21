movieData = LOAD '/home/chetanchandak/Documents/ml-latest/movies.csv' using PigStorage(',') as (movieId:int, movieName:chararray, genre:chararray);
ratingsData = LOAD '/home/chetanchandak/Documents/ml-latest/ratings.csv' using PigStorage(',') as (userId:int, movieId:int, rating:float, timestamp:long);

-- replace '|' with '*' to tokenize
movieGenreCleaning = FOREACH movieData GENERATE $0, $1,REPLACE($2,'\\|','*') as genre;
moviesGenre = FOREACH movieGenreCleaning GENERATE $0, $1, FLATTEN(TOKENIZE($2)) as genre;
movieRatings = JOIN ratingsData by movieId, moviesGenre by movieId;

genreRatingData = FOREACH movieRatings GENERATE $5 as movieName, $6 as movieGenre, $2 as rating;
fantasyGenre = FILTER genreRatingData by movieGenre == 'Fantasy';

groupByTitle =  GROUP fantasyGenre by movieName;
averageRatingOfEachFantasyMovie = FOREACH groupByTitle GENERATE $0 as movieName, $1.movieGenre as movieGenre, AVG($1.rating) as averageMovieRating;


orderFantasyMoviesByRatingsDesc = ORDER averageRatingOfEachFantasyMovie by averageMovieRating DESC;
top10FantasyMovies = LIMIT orderFantasyMoviesByRatingsDesc 10;

STORE top10FantasyMovies into '/home/chetanchandak/Documents/Top10FantasyMovies' using PigStorage();

