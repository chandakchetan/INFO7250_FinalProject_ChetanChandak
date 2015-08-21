movieRatings = LOAD '/home/chetanchandak/Documents/ml-latest/JoinedTables.csv' using PigStorage(',') as (userId:int, movieId:int, ratings:float, movieName:chararray, year:chararray, genre:chararray);
groupByTitle =  GROUP movieRatings by movieName;
averageRatingOfEachMovie = FOREACH groupByTitle GENERATE $0, AVG(movieRatings.ratings) as averageMovieRating;
averageMovies = FILTER averageRatingOfEachMovie by $1>1 AND $1<3.5;
fileGroup= GROUP averageMovies ALL;
averageCounter = FOREACH fileGroup GENERATE COUNT(averageMovies);
STORE averageCounter into '/home/chetanchandak/Documents/AverageCounter' using PigStorage();
