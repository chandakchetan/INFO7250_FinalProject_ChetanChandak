movieData = LOAD '/home/chetanchandak/Documents/ml-latest/movies.csv' using PigStorage(',') as (movieId:int, movieName:chararray, genre:chararray);
ratingsData = LOAD '/home/chetanchandak/Documents/ml-latest/ratings.csv' using PigStorage(',') as (userId:int, movieId:int, rating:float, timestamp:long);

-- replace '|' with '*' to tokenize
movieGenreCleaning = FOREACH movieData GENERATE $0, $1,REPLACE($2,'\\|','*') as genre;
moviesGenre = FOREACH movieGenreCleaning GENERATE $0, $1, FLATTEN(TOKENIZE($2)) as genre;
movieRatings = JOIN ratingsData by movieId, moviesGenre by movieId;

genreRatingData = FOREACH movieRatings GENERATE $5 as movieName, $6 as movieGenre, $2 as rating;
documentaryGenre = FILTER genreRatingData by movieGenre == 'Documentary';

groupByTitle =  GROUP documentaryGenre by movieName;
averageRatingOfEachDocumentaryMovie = FOREACH groupByTitle GENERATE $0 as movieName, $1.movieGenre as movieGenre, AVG($1.rating) as averageMovieRating;


orderDocumentaryMoviesByRatingsDesc = ORDER averageRatingOfEachDocumentaryMovie by averageMovieRating DESC;
top10DocumentaryMovies = LIMIT orderDocumentaryMoviesByRatingsDesc 10;

STORE top10DocumentaryMovies into '/home/chetanchandak/Documents/Top10DocumentaryMovies' using PigStorage();

