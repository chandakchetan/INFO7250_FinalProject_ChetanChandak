movieRatings = LOAD '/home/chetanchandak/Documents/ml-latest/JoinedTables.csv' using PigStorage(',') as (userId:int, movieId:int, ratings:float, movieName:chararray, year:chararray, genre:chararray);
groupByTitle =  GROUP movieRatings by movieName;
averageRatingOfEachMovie = FOREACH groupByTitle GENERATE $0, AVG(movieRatings.ratings) as averageMovieRating;
popularMovies = FILTER averageRatingOfEachMovie by $1>=3.5;
fileGroup= GROUP popularMovies ALL;
popularCounter = FOREACH fileGroup GENERATE COUNT(popularMovies);
STORE popularCounter into '/home/chetanchandak/Documents/PopularCounter' using PigStorage();
