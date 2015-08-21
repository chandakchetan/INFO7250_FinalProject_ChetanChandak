movieRatings = LOAD '/home/chetanchandak/Documents/ml-latest/JoinedTables.csv' using PigStorage(',') as (userId:int, movieId:int, ratings:float, movieName:chararray, year:chararray, genre:chararray);
groupByTitle =  GROUP movieRatings by movieName;
averageRatingOfEachMovie = FOREACH groupByTitle GENERATE $0, AVG(movieRatings.ratings) as averageMovieRating;
flopMovies = FILTER averageRatingOfEachMovie by $1<=1;
fileGroup= GROUP flopMovies ALL;
flopCounter = FOREACH fileGroup GENERATE COUNT(flopMovies);
STORE flopCounter into '/home/chetanchandak/Documents/FlopCounter' using PigStorage();
