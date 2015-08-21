movieData = LOAD '/home/chetanchandak/Documents/ml-latest/movies.csv' using PigStorage(',') as (movieId:int, movieName:chararray, genre:chararray);
ratingsData = LOAD '/home/chetanchandak/Documents/ml-latest/ratings.csv' using PigStorage(',') as (userId:int, movieId:int, rating:float, timestamp:long);
movieRatings = JOIN ratingsData by movieId, movieData by movieId;
groupByTitle =  GROUP movieRatings by movieName;
averageRatingOfEachMovie = FOREACH groupByTitle GENERATE $0, AVG(movieRatings.rating) as averageMovieRating;
orderByAverageRatingAsc = ORDER averageRatingOfEachMovie by averageMovieRating ASC;
Worst100_MovieRatings = LIMIT orderByAverageRatingAsc 100;
STORE Worst100_MovieRatings into '/home/chetanchandak/Documents/Worst100MovieRatings' using PigStorage();
