movieData = LOAD '/home/chetanchandak/Documents/ml-latest/movies.csv' using PigStorage(',') as (movieId:int, movieName:chararray, genre:chararray);
ratingsData = LOAD '/home/chetanchandak/Documents/ml-latest/ratings.csv' using PigStorage(',') as (userId:int, movieId:int, rating:float, timeStamp:long);
movieRatings = JOIN ratingsData by movieId, movieData by movieId;
groupByTitle =  GROUP movieRatings by movieName;
averageRatingOfEachMovie = FOREACH groupByTitle GENERATE $0, AVG(movieRatings.rating) as averageMovieRating;
orderByAverageRatingDesc = ORDER averageRatingOfEachMovie by averageMovieRating DESC;
best100_MovieRatings = LIMIT orderByAverageRatingDesc 100;
STORE best100_MovieRatings into '/home/chetanchandak/Documents/Best100MovieRatings' using PigStorage();
